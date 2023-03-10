import datetime
from dawgpath_data_pipeline.models.base import Base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Text, ForeignKey, Integer, DateTime, String


class Graph(Base):
    course_id = Column(Integer, ForeignKey('course.id', ondelete="CASCADE"))
    course = relationship("Course", back_populates="graph")
    graph_json = Column(Text())
    create_date = Column(DateTime, default=datetime.datetime.utcnow)


class CurricGraph(Base):
    abbrev = Column(String(length=6))
    graph_json = Column(Text())
    create_date = Column(DateTime, default=datetime.datetime.utcnow)
