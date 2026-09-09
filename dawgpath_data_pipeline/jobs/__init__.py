from dawgpath_data_pipeline.databases.implementation import get_db_implementation


class JobResult:
    def __init__(self, job_name, status="SUCCESS", rows_affected=0, metadata=None):
        self.job_name = job_name
        self.status = status
        self.rows_affected = rows_affected
        self.metadata = metadata if metadata is not None else {}

    def __getitem__(self, item):
        if hasattr(self, item):
            return getattr(self, item)
        return self.metadata[item]

    def get(self, item, default=None):
        if hasattr(self, item):
            return getattr(self, item)
        return self.metadata.get(item, default)

    def to_dict(self):
        return {
            "job_name": self.job_name,
            "status": self.status,
            "rows_affected": self.rows_affected,
            "metadata": self.metadata,
        }

    def __repr__(self):
        return (f"JobResult(job_name='{self.job_name}', status='{self.status}', "
                f"rows_affected={self.rows_affected}, metadata={self.metadata})")


class DataJob:
    session = None

    def __init__(self):
        db = get_db_implementation()
        self.session = db.get_session()

    def _create_result(self, rows_affected=0, status="SUCCESS", metadata=None):
        return JobResult(
            job_name=self.__class__.__name__,
            status=status,
            rows_affected=rows_affected,
            metadata=metadata,
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

    def _delete_objects(self, to_delete):
        q = self.session.query(to_delete)
        q.delete()
        self.session.commit()
