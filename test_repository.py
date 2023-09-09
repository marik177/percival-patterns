import model
import repository
from sqlalchemy.orm import Session
from sqlalchemy.util import deprecations

deprecations.SILENCE_UBER_WARNING = 1


def test_repository_can_save_a_batch(session: Session):
    batch = model.Batch("batch1", "RUSTY-SOAPDISH", 100, eta=None)

    repo = repository.SqlAlchemyRepository(session)
    repo.add(batch)
    session.commit()
    rows = session.execute(
        "SELECT reference, sku, _purchased_quantity, eta FROM batches"
    )
    assert list(rows) == [("batch1", "RUSTY-SOAPDISH", 100, None)]
