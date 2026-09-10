# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy.orm import Session


class Database:
    engine = None
    session = None

    def get_session(self):
        if self.session is None:
            self.session = Session(self.engine)
        return self.session
