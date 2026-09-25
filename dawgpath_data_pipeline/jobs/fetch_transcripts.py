# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import or_

from dawgpath_data_pipeline.dao.edw import get_transcripts_in_year_quarter
from dawgpath_data_pipeline.jobs import DataJob
from dawgpath_data_pipeline.models.transcript import Transcript
from dawgpath_data_pipeline.utilities import (
    get_combined_term,
    get_history_start_term,
)


class FetchTranscriptData(DataJob):
    upstream_sources = ["EDW: sec.transcript"]

    # Replaces one quarter and prunes quarters that aged out of the window
    def run(self, year, quarter):
        term = get_combined_term(year, quarter)
        rows_affected = self._atomic_replace_where(
            Transcript,
            or_(Transcript.combined_qtr == term,
                Transcript.combined_qtr < get_history_start_term()),
            self._get_transcript_mappings(year, quarter),
            lock_key=f"{Transcript.__tablename__}:{term}")
        return self._create_result(rows_affected=rows_affected)

    def _get_transcript_mappings(self, year, quarter):
        transcripts = get_transcripts_in_year_quarter(year, quarter)

        mappings = []
        for transcript in transcripts.to_dict('records'):
            mapping = {
                "system_key": transcript['system_key'],
                "tran_yr": transcript['tran_yr'],
                "tran_qtr": transcript['tran_qtr'],
                "combined_qtr": get_combined_term(transcript['tran_yr'],
                                                  transcript['tran_qtr']),
            }

            """
            handle manual override cases:

            Manual Override of the student's Quarterly Grade Points Total i.e.
             when this field is greater than zero, its value is used instead of
             qtr_grade_points
            """
            if(transcript['over_qtr_grade_pt'] > 0):
                mapping["qtr_grade_points"] = transcript['over_qtr_grade_pt']
            else:
                mapping["qtr_grade_points"] = transcript['qtr_grade_points']

            if (transcript['over_qtr_grade_at'] > 0):
                mapping["qtr_graded_attmp"] = transcript['over_qtr_grade_at']
            else:
                mapping["qtr_graded_attmp"] = transcript['qtr_graded_attmp']

            mappings.append(mapping)
        return mappings

    def _get_transcripts(self, year, quarter):
        return [Transcript(**mapping) for mapping
                in self._get_transcript_mappings(year, quarter)]

    # delete existing transcript data
    def _delete_transcripts(self):
        self._delete_objects(Transcript)
