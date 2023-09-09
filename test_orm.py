from datetime import date

import model
from sqlalchemy.orm import Session
from sqlalchemy.util import deprecations
from sqlalchemy import text

deprecations.SILENCE_UBER_WARNING = 1


def test_orderline_mapper_can_load_lines(session: Session):
    # session.execute(
    #     "INSERT INTO order_lines (orderid, sku, qty) VALUES "
    #     '("order1", "RED-CHAIR", 12),'
    #     '("order1", "RED-TABLE", 13),'
    #     '("order2", "BLUE-LIPSTICK", 14)'
    # )

    session.execute(
        text("INSERT INTO order_lines (orderid, sku, qty) VALUES (:orderid, :sku, :qty)"),
        [
            {'orderid': "order1", 'sku': "RED-CHAIR", 'qty': 12},
            {'orderid': "order1", 'sku': "RED-TABLE", 'qty': 13},
            {'orderid': "order2", 'sku': "BLUE-LIPSTICK", 'qty': 14},
        ]
    )
    expected = [
        model.OrderLine("order1", "RED-CHAIR", 12),
        model.OrderLine("order1", "RED-TABLE", 13),
        model.OrderLine("order2", "BLUE-LIPSTICK", 14),
    ]
    assert session.query(model.OrderLine).all() == expected


def test_orderline_mapper_can_save_lines(session):
    new_line = model.OrderLine("order1", "DECORATIVE-WIDGET", 12)
    session.add(new_line)
    session.commit()

    rows = list(session.execute("SELECT orderid, sku, qty FROM order_lines"))

    assert rows == [("order1", "DECORATIVE-WIDGET", 12)]


def test_retrieving_batches(session):
    session.execute(
        "INSERT INTO batches (reference, sku, _purchased_quantity, eta) VALUES "
        "('batch1', 'sku1', 100, null)"
    )
    session.execute(
        "INSERT INTO batches (reference, sku, _purchased_quantity, eta) VALUES "
        "('batch2', 'sku2', 200, '2023-07-13')"
    )
    expected = [
        model.Batch('batch1', 'sku1', 100, eta=None),
        model.Batch('batch2', 'sku2', 200, eta=date(2023, 7, 13))
    ]
    assert session.query(model.Batch).all() == expected


def test_saving_batches(session):
    batch = model.Batch("batch1", "sku1", 100, eta=None)
    print(batch.__dict__)
    session.add(batch)
    session.commit()
    rows = list(
        session.execute(
            'SELECT reference, sku, _purchased_quantity, eta FROM "batches"'
        )
    )
    assert rows == [("batch1", "sku1", 100, None)]


def test_saving_allocations(session):
    batch = model.Batch("batch1", "sku1", 100, eta=None)
    line = model.OrderLine("order1", "sku1", 10)
    batch.allocate(line)
    session.add(batch)
    session.commit()
    rows = list(session.execute("SELECT orderline_id, batch_id FROM allocations"))
    assert rows == [(line.id, batch.id)]


def test_retrieving_allocations(session):
    session.execute(
        'INSERT INTO order_lines (orderid, sku, qty) VALUES ("order1", "sku1", 10)'
    )
    [[olid]] = session.execute(
        'SELECT id FROM order_lines WHERE orderid=:orderid AND sku=:sku',
        dict(orderid='order1', sku='sku1')
    )
    session.execute(
        'INSERT INTO batches (reference, sku, _purchased_quantity, eta) VALUES '
        '("batch1", "sku1", 100, null)'
    )
    [[bid]] = session.execute(
        'SELECT id FROM batches WHERE reference=:ref AND sku=:sku',
        dict(ref="batch1", sku="sku1")
    )
    session.execute(
        "INSERT INTO allocations(orderline_id, batch_id) VALUES "
        "(:orderline_id, :batch_id)", dict(orderline_id=olid, batch_id=bid)
    )

    batch = session.query(model.Batch).one()
    assert batch._allocations == {model.OrderLine("order1", "sku1", 10)}