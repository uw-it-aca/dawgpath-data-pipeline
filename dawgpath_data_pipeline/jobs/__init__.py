from dawgpath_data_pipeline.databases.implementation import get_db_implemenation


class DataJob:
    session = None

    def __init__(self):
        db = get_db_implemenation()
        self.session = db.get_session()

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
