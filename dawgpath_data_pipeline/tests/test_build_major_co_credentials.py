# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from dawgpath_data_pipeline.jobs.build_major_co_credentials import BuildMajorCoCredentials
from dawgpath_data_pipeline.models.regis_major import RegisMajor
from dawgpath_data_pipeline.tests import DBTest


class TestBuildMajorCoCredentials(DBTest):

    def setUp(self):
        super().setUp()
        # Add 12 students declared in MATH and AMATH in the same term
        for i in range(12):
            syskey = 4000 + i
            self.session.add(RegisMajor(
                system_key=syskey,
                regis_yr=2025,
                regis_qtr=1,
                regis_term=20251,
                regis_major_abbr="MATH"
            ))
            self.session.add(RegisMajor(
                system_key=syskey,
                regis_yr=2025,
                regis_qtr=1,
                regis_term=20251,
                regis_major_abbr="AMATH"
            ))
        self.session.commit()

    def test_build_major_co_credentials(self):
        job = BuildMajorCoCredentials()
        result = job.run()
        self.assertEqual(result.rows_affected, 2)

        records = job.build_all_co_credentials()
        math_rec = next(r for r in records if r.major == "MATH")
        self.assertEqual(len(math_rec.co_majors), 1)
        self.assertEqual(math_rec.co_majors[0]["major_abbr"], "AMATH")
        self.assertEqual(math_rec.co_majors[0]["percent_students"], 100.0)
