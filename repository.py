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


class SqlRepository(AbstractRepository):
    def __init__(self, session: Session):
        self.session = session

    def get(self, reference):
        pass

    def add(self, batch: model.Batch):
        pass







