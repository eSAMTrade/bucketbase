import shutil
import tempfile
import threading
from queue import Queue
from pathlib import PurePosixPath
from typing import BinaryIO, List, Tuple, Any, Callable
from abc import ABC, abstractmethod
from bucketbase import IBucket
from enum import Enum, auto


class SuccessCriteria(Enum):
    ANY = auto()  # At least one bucket write succeeds
    MAJORITY = auto()  # At least n/2+1 bucket writes succeed
    ALL = auto()  # All bucket writes succeed


class MirrorOperationError(RuntimeError):
    """Exception raised when a mirroring operation fails to meet success criteria"""

    def __init__(self, message, errors=None):
        super().__init__(message)
        self.errors = errors or []


class ReadQueue(BinaryIO):
    """File-like object that reads from a queue"""

    def __init__(self, queue):
        self.queue = queue
        self.buffer = b""
        self._closed = False
        self._eof_reached = False

    @property
    def closed(self):
        return self._closed

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self._closed = True

    def read(self, size=-1):
        if self.closed:
            return b""

        if self._eof_reached and not self.buffer:
            return b""

        if size < 0:  # Read everything
            result = self.buffer
            while not self._eof_reached:
                chunk = self.queue.get()
                self.queue.task_done()
                if chunk is None:  # EOF marker
                    self._eof_reached = True
                    break
                result += chunk
            self.buffer = b""
            return result
        else:
            while len(self.buffer) < size and not self._eof_reached:
                chunk = self.queue.get()
                self.queue.task_done()
                if chunk is None:  # EOF marker
                    self._eof_reached = True
                    break
                self.buffer += chunk

            result = self.buffer[:size]
            self.buffer = self.buffer[size:]
            return result


class SuccessEvaluator:
    @staticmethod
    def evaluate(success_count: int, total_buckets: int, success_criteria: SuccessCriteria, errors: list = None) -> None:
        """Evaluate if the operation met the success criteria"""
        if success_criteria == SuccessCriteria.ALL and success_count < total_buckets:
            failed_indices = [i for i, _ in errors] if errors else []
            raise MirrorOperationError(f"Failed to write to all buckets. Failed bucket indices: {failed_indices}", errors)

        elif success_criteria == SuccessCriteria.MAJORITY and success_count <= total_buckets // 2:
            raise MirrorOperationError(f"Failed to write to a majority of buckets. " f"Success: {success_count}, Required: {total_buckets // 2 + 1}", errors)

        elif success_criteria == SuccessCriteria.ANY and success_count == 0:
            raise MirrorOperationError("Failed to write to any bucket", errors)


class MirrorStrategy(ABC):
    @abstractmethod
    def mirror_stream(self, name: str, stream: BinaryIO, buckets: List[IBucket], success_criteria: SuccessCriteria) -> None:
        pass


class BaseMirrorStrategy(MirrorStrategy, ABC):
    def execute_on_buckets(
        self, buckets: List[IBucket], operation: Callable[[IBucket], None], success_criteria: SuccessCriteria
    ) -> Tuple[int, List[Tuple[int, Exception]]]:
        """Execute operation on each bucket and evaluate success criteria"""
        errors = []
        success_count = 0

        for i, bucket in enumerate(buckets):
            try:
                operation(bucket)
                success_count += 1
            except Exception as e:
                errors.append((i, e))

        SuccessEvaluator.evaluate(success_count, len(buckets), success_criteria, errors)
        return success_count, errors


class ParallelStrategy(BaseMirrorStrategy):
    def mirror_stream(self, name: str, stream: BinaryIO, buckets: List[IBucket], success_criteria: SuccessCriteria) -> None:
        content = stream.read()
        errors = []
        success_count = 0
        completion_lock = threading.Lock()

        def write_to_bucket(bucket, content, bucket_index):
            nonlocal success_count
            try:
                bucket.put_object(name, content)
                with completion_lock:
                    success_count += 1
            except Exception as e:
                errors.append((bucket_index, e))

        threads = []
        for i, bucket in enumerate(buckets):
            thread = threading.Thread(target=write_to_bucket, args=(bucket, content, i))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        SuccessEvaluator.evaluate(success_count, len(buckets), success_criteria, errors)


class InMemoryStrategy(BaseMirrorStrategy):
    def mirror_stream(self, name: str, stream: BinaryIO, buckets: List[IBucket], success_criteria: SuccessCriteria) -> None:
        content = stream.read()

        def write_operation(bucket):
            bucket.put_object(name, content)

        self.execute_on_buckets(buckets, lambda bucket: bucket.put_object(name, content), success_criteria)


class TempFileStrategy(BaseMirrorStrategy):
    def mirror_stream(self, name: str, stream: BinaryIO, buckets: List[IBucket], success_criteria: SuccessCriteria) -> None:
        with tempfile.NamedTemporaryFile() as tmp:
            shutil.copyfileobj(stream, tmp)
            tmp.flush()

            def write_operation(bucket):
                tmp.seek(0)
                bucket.put_object_stream(name, tmp)

            self.execute_on_buckets(buckets, write_operation, success_criteria)


class StreamingStrategy(BaseMirrorStrategy):
    """Strategy that streams to multiple buckets using queues without buffering everything in memory or on disk"""

    def __init__(self, buffer_size=8192, queue_size=100, timeout=30):
        self.buffer_size = buffer_size
        self.queue_size = queue_size
        self.timeout = timeout

    def mirror_stream(self, name: str, stream: BinaryIO, buckets: List[IBucket], success_criteria: SuccessCriteria) -> None:
        # Fast path for single bucket
        if len(buckets) == 1:
            buckets[0].put_object_stream(name, stream)
            return

        data_queue = Queue(maxsize=self.queue_size)
        end_marker = object()  # Unique end marker
        errors = []
        success_counter = [0]  # Use a mutable object to track success count
        completion_lock = threading.Lock()

        reader_thread = self._create_reader_thread(stream, data_queue, end_marker, errors)
        bucket_queues = [Queue(maxsize=10) for _ in range(len(buckets))]
        distributor_thread = self._create_distributor_thread(data_queue, bucket_queues, end_marker, errors)
        writer_threads = self._create_writer_threads(buckets, name, bucket_queues, completion_lock, success_counter, errors)

        # Wait for completion
        reader_thread.join(timeout=self.timeout)
        distributor_thread.join(timeout=self.timeout)
        for thread in writer_threads:
            thread.join(timeout=self.timeout)

        SuccessEvaluator.evaluate(success_counter[0], len(buckets), success_criteria, errors)

    def _create_reader_thread(self, stream, data_queue, end_marker, errors):
        def reader_task():
            try:
                while True:
                    chunk = stream.read(self.buffer_size)
                    if not chunk:  # End of stream
                        data_queue.put(end_marker)
                        break
                    data_queue.put(chunk)
            except Exception as e:
                data_queue.put(end_marker)  # Signal end on error
                errors.append(("reader", e))

        thread = threading.Thread(target=reader_task)
        thread.daemon = True
        thread.start()
        return thread

    def _create_distributor_thread(self, data_queue, bucket_queues, end_marker, errors):
        def distributor_task():
            try:
                while True:
                    item = data_queue.get()
                    if item is end_marker:
                        # Pass end marker to all bucket queues
                        for q in bucket_queues:
                            q.put(None)
                        break

                    # Distribute to all bucket queues
                    for q in bucket_queues:
                        q.put(item)
            except Exception as e:
                # Signal end to all queues on error
                for q in bucket_queues:
                    q.put(None)
                errors.append(("distributor", e))

        thread = threading.Thread(target=distributor_task)
        thread.daemon = True
        thread.start()
        return thread

    def _create_writer_threads(self, buckets, name, bucket_queues, completion_lock, success_counter, errors):
        writer_threads = []

        def writer_task(_bucket, q, bucket_index):
            try:
                with ReadQueue(q) as reader:
                    _bucket.put_object_stream(name, reader)
                with completion_lock:
                    success_counter[0] += 1
            except Exception as e:
                errors.append((bucket_index, e))

        for i, bucket in enumerate(buckets):
            thread = threading.Thread(target=writer_task, args=(bucket, bucket_queues[i], i))
            thread.daemon = True
            writer_threads.append(thread)
            thread.start()

        return writer_threads


class MirrorBucket(IBucket):
    def __init__(self, buckets: List[IBucket], strategy: MirrorStrategy = None, success_criteria: SuccessCriteria = SuccessCriteria.ALL) -> None:
        if not buckets:
            raise ValueError("At least one bucket is required")
        self._buckets = buckets
        self._read_bucket = buckets[0]
        self._strategy = strategy or InMemoryStrategy()
        self._success_criteria = success_criteria

    def put_object(self, name: PurePosixPath | str, content: bytes) -> None:
        _name = self._validate_name(name)
        import io

        stream = io.BytesIO(content)
        self._strategy.mirror_stream(_name, stream, self._buckets, self._success_criteria)

    def put_object_stream(self, name: PurePosixPath | str, stream: BinaryIO) -> None:
        _name = self._validate_name(name)
        self._strategy.mirror_stream(_name, stream, self._buckets, self._success_criteria)

    def get_object(self, name: PurePosixPath | str) -> bytes:
        _name = self._validate_name(name)
        return self._read_bucket.get_object(_name)

    def get_object_stream(self, name: PurePosixPath | str) -> BinaryIO:
        _name = self._validate_name(name)
        return self._read_bucket.get_object_stream(_name)

    def exists(self, name: PurePosixPath | str) -> bool:
        _name = self._validate_name(name)
        return self._read_bucket.exists(_name)

    def get_size(self, name: PurePosixPath | str) -> int:
        _name = self._validate_name(name)
        return self._read_bucket.get_size(_name)

    def list_objects(self, prefix: PurePosixPath | str = "") -> List[str]:
        _prefix = self._validate_name(prefix) if prefix else ""
        return self._read_bucket.list_objects(_prefix)

    def shallow_list_objects(self, prefix: PurePosixPath | str = "", delimiter: str = "/") -> List[str]:
        _prefix = self._validate_name(prefix) if prefix else ""
        return self._read_bucket.shallow_list_objects(_prefix, delimiter)

    def remove_objects(self, names: List[PurePosixPath | str]) -> None:
        _names = [self._validate_name(name) for name in names]

        def remove_operation(bucket):
            bucket.remove_objects(_names)

        errors = []
        success_count = 0

        for i, bucket in enumerate(self._buckets):
            try:
                bucket.remove_objects(_names)
                success_count += 1
            except Exception as e:
                errors.append((i, e))

        SuccessEvaluator.evaluate(success_count, len(self._buckets), self._success_criteria, errors)
