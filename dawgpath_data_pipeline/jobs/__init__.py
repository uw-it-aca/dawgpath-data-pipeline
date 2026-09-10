from dawgpath_data_pipeline.databases.implementation import get_db_implementation


import time
from datetime import datetime, timezone
from sqlalchemy import insert
from dawgpath_data_pipeline.databases.implementation import get_db_implementation
from dawgpath_data_pipeline import MINIMUM_DATA_COUNT


class JobResult:
    def __init__(
        self,
        job_name,
        status="SUCCESS",
        rows_affected=0,
        start_time=None,
        end_time=None,
        duration_seconds=0.0,
        upstream_sources=None,
        output_artifact_uri=None,
        exception_details=None,
        metadata=None,
    ):
        self.job_name = job_name
        self.status = status
        self.rows_affected = rows_affected
        self.start_time = start_time
        self.end_time = end_time
        self.duration_seconds = duration_seconds
        self.upstream_sources = upstream_sources if upstream_sources is not None else []
        self.output_artifact_uri = output_artifact_uri
        self.exception_details = exception_details
        self.metadata = metadata if metadata is not None else {}

    def __getitem__(self, item):
        if hasattr(self, item):
            return getattr(self, item)
        return self.metadata[item]

    def get(self, item, default=None):
        if hasattr(self, item):
            val = getattr(self, item)
            if val is not None:
                return val
        return self.metadata.get(item, default)

    def to_dict(self):
        return {
            "job_name": self.job_name,
            "status": self.status,
            "rows_affected": self.rows_affected,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": self.duration_seconds,
            "upstream_sources": self.upstream_sources,
            "output_artifact_uri": self.output_artifact_uri,
            "exception_details": self.exception_details,
            "privacy_threshold": MINIMUM_DATA_COUNT,
            "metadata": self.metadata,
        }

    def __repr__(self):
        return (
            f"JobResult(job_name='{self.job_name}', status='{self.status}', "
            f"rows_affected={self.rows_affected}, duration_seconds={self.duration_seconds:.2f}s, "
            f"metadata={self.metadata})"
        )


class DataJob:
    session = None
    upstream_sources = []

    def __init__(self):
        db = get_db_implementation()
        self.session = db.get_session()
        self._start_time_iso = datetime.now(timezone.utc).isoformat()
        self._start_time_ticks = time.time()

    def _create_result(
        self,
        rows_affected=0,
        status="SUCCESS",
        output_artifact_uri=None,
        exception_details=None,
        metadata=None,
    ):
        end_time_ticks = time.time()
        end_time_iso = datetime.now(timezone.utc).isoformat()
        duration = end_time_ticks - self._start_time_ticks

        meta_dict = metadata.copy() if metadata is not None else {}
        meta_dict["privacy_threshold"] = MINIMUM_DATA_COUNT

        return JobResult(
            job_name=self.__class__.__name__,
            status=status,
            rows_affected=rows_affected,
            start_time=self._start_time_iso,
            end_time=end_time_iso,
            duration_seconds=duration,
            upstream_sources=getattr(self, "upstream_sources", []),
            output_artifact_uri=output_artifact_uri,
            exception_details=exception_details,
            metadata=meta_dict,
        )

    def _bulk_save_objects(self, objects, chunk_size=10000):
        try:
            chunks = [objects[x:x + chunk_size] for x in
                      range(0, len(objects), chunk_size)]

            for chunk in chunks:
                self.session.add_all(chunk)
                self.session.commit()
        except TypeError:
            pass

    def _delete_objects(self, to_delete, commit=True):
        q = self.session.query(to_delete)
        q.delete()
        if commit:
            self.session.commit()

    def _atomic_replace(self, model_cls, objects, chunk_size=10000):
        """
        Atomically deletes existing rows for model_cls and saves replacement objects
        within a single transaction. If saving fails, changes are rolled back.
        """
        try:
            self._delete_objects(model_cls, commit=False)
            chunks = [objects[x:x + chunk_size] for x in
                      range(0, len(objects), chunk_size)]
            for chunk in chunks:
                self.session.add_all(chunk)
            self.session.commit()
        except Exception:
            self.session.rollback()
            raise

    def _atomic_replace_stream(self, model_cls, mapping_batches,
                               chunk_size=10000):
        """
        Atomically replaces all rows for model_cls from an iterable of
        column-mapping batches. Rows are inserted without building ORM
        instances, so tables too large to materialize at once stay within
        memory. Returns the number of rows inserted.
        """
        rows_affected = 0
        try:
            self._delete_objects(model_cls, commit=False)
            for batch in mapping_batches:
                for x in range(0, len(batch), chunk_size):
                    chunk = batch[x:x + chunk_size]
                    if chunk:
                        self.session.execute(insert(model_cls), chunk)
                        rows_affected += len(chunk)
            self.session.commit()
        except Exception:
            self.session.rollback()
            raise
        return rows_affected
