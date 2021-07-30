from prereq_data_pipeline.dao.edw import get_transcripts_since_year
from prereq_data_pipeline.models.transcript import Transcript
from prereq_data_pipeline.databases.implementation import get_db_implemenation
from prereq_data_pipeline.utilities import get_combined_term


def run():
    db = get_db_implemenation()
    session = db.get_session()

    _delete_transcripts(session)
    transcripts = _get_transcripts()
    _save_transcripts(session, transcripts)


# get transcript data
def _get_transcripts():
    transcripts = get_transcripts_since_year(2016)

    transcript_objects = []
    for index, transcript in transcripts.iterrows():
        combined_qtr = get_combined_term(transcript['tran_yr'],
                                         transcript['tran_qtr'])
        transcript_obj = Transcript(
            system_key=transcript['system_key'],
            tran_yr=transcript['tran_yr'],
            tran_qtr=transcript['tran_qtr'],
            combined_qtr=combined_qtr
        )

        """
        handle manual override cases:

        Manual Override of the student's Quarterly Grade Points Total i.e.
         when this field is greater than zero, its value is used instead of
         qtr_grade_points
        """
        if(transcript['over_qtr_grade_pt'] > 0):
            transcript_obj.qtr_grade_points = transcript['over_qtr_grade_pt']
        else:
            transcript_obj.qtr_grade_points = transcript['qtr_grade_points']

        if (transcript['over_qtr_grade_at'] > 0):
            transcript_obj.qtr_graded_attmp = transcript['over_qtr_grade_at']
        else:
            transcript_obj.qtr_graded_attmp = transcript['qtr_graded_attmp']

        transcript_objects.append(transcript_obj)
    return transcript_objects


# save transcript data
def _save_transcripts(session, transcripts):
    chunk_size = 10000
    chunks = [transcripts[x:x + chunk_size] for x in
              range(0, len(transcripts), chunk_size)]

    for chunk in chunks:
        session.add_all(chunk)
        session.commit()


# delete existing transcript data
def _delete_transcripts(session):
    q = session.query(Transcript)
    q.delete()
    session.commit()
