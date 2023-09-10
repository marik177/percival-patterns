import model
import repository
from sqlalchemy.orm import Session
from sqlalchemy.util import deprecations
from repository import get_allocations

deprecations.SILENCE_UBER_WARNING = 1


def test_repository_can_save_a_batch(session: Session):
    batch = model.Batch("batch1", "RUSTY-SOAPDISH", 100, eta=None)

    repo = repository.SqlRepository(session)
    repo.add(batch)
    session.commit()
    rows = session.execute(
        "SELECT reference, sku, _purchased_quantity, eta FROM batches"
    )
    assert list(rows) == [("batch1", "RUSTY-SOAPDISH", 100, None)]


def insert_order_line(session):
    session.execute(
        "INSERT INTO order_lines (orderid, sku, qty) VALUES "
        "('order1', 'GENERIC-SOFA', 12)"
    )
    [[order_line_id]] = session.execute(
        "SELECT id FROM order_lines WHERE orderid=:orderid AND sku=:sku",
        dict(orderid='order1', sku='GENERIC-SOFA')
    )
    return order_line_id


def insert_batch(session, batch_ref):
    session.execute(
        "INSERT INTO batches (reference, sku, _purchased_quantity, eta) VALUES "
        "(:reference,'GENERIC-SOFA', 100, null)",
        dict(reference=batch_ref)
    )
    [[batch_id]] = session.execute(
        "SELECT id FROM batches WHERE reference=:batch_ref AND sku='GENERIC-SOFA'",
        dict(batch_ref=batch_ref, sku='GENERIC-SOFA')
    )
    return batch_id


def insert_allocation(session, batch_id, orderline_id):
    session.execute(
        "INSERT INTO allocations (orderline_id, batch_id) VALUES"
        "(:orderline_id, :batch_id)",
        dict(orderline_id=orderline_id, batch_id=batch_id)
    )


def test_repository_can_retrieve_a_batch_with_allocations(session):
    orderline_id = insert_order_line(session)
    batch_1_id = insert_batch(session, 'batch1')
    insert_batch(session, 'batch2')
    insert_allocation(session, batch_1_id, orderline_id)

    repo = repository.SqlRepository(session)
    retrieved = repo.get('batch1')
    expected = model.Batch("batch1", "GENERIC-SOFA", 100, eta=None)
    assert retrieved == expected  # Batch.__eq__ only compares reference
    assert retrieved.sku == expected.sku
    assert retrieved._purchased_quantity == expected._purchased_quantity
    assert retrieved._allocations == {
        model.OrderLine("order1", "GENERIC-SOFA", 12),
    }


def test_updating_a_batch(session):
    order1 = model.OrderLine("order1", "WEATHERED-BENCH", 10)
    order2 = model.OrderLine("order2", "WEATHERED-BENCH", 20)
    batch = model.Batch("batch1", "WEATHERED-BENCH", 100, eta=None)
    batch.allocate(order1)

    repo = repository.SqlRepository(session)
    repo.add(batch)
    session.commit()

    batch.allocate(order2)
    repo.add(batch)
    session.commit()
    assert get_allocations(session, batch.reference) == {"order1", "order2"}



