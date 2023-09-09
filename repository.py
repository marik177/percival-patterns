import abc
from sqlalchemy.orm import Session
import model


class AbstractRepository(abc.ABC):
    @abc.abstractmethod
    def get(self, reference) -> model.Batch:
        pass

    @abc.abstractmethod
    def add(self, batch: model.Batch):
        pass


class SqlAlchemyRepository(AbstractRepository):
    def __init__(self, session: Session):
        self.session = session

    def get(self, reference):
        return self.session.query(model.Batch).filter_by(reference=reference).one()

    def add(self, batch: model.Batch):
        return self.session.add(batch)

    def list(self):
        return self.session.query(model.Batch).all()





