from unittest import TestCase
from unittest.mock import MagicMock, call, patch

from gobeventproducer.database.gob.contextmanager import GobDatabaseConnection
from gobeventproducer.utils.relations import RelationInfo


class MockComp:
    asc = MagicMock()

    def __init__(self, name):
        self.name = name

    def __eq__(self, other):
        return f"{self.name} == {other}"

    def __gt__(self, other):
        return f"{self.name} > {other}"

    def __le__(self, other):
        return f"{self.name} <= {other}"

class MockRelationInfoBuilder:
    @classmethod
    def build(cls, catalogue: str, collection: str):
        return {
            "some_rel": RelationInfo(
                relation_table_name="rel_table_for_some_rel",
                dst_table_name="dst_table_1"
            ),
            "some_other_rel": RelationInfo(
                relation_table_name="rel_table_for_some_other_rel",
                dst_table_name="dst_table_1"
            ),
        }

@patch("gobeventproducer.database.gob.contextmanager.RelationInfoBuilder", MockRelationInfoBuilder)
class TestGobDatabaseConnection(TestCase):
    def test_context_manager(self):
        inst = GobDatabaseConnection("", "", MagicMock())
        inst._connect = MagicMock()
        inst.session = MagicMock()

        with inst as tst:
            self.assertEqual(tst, inst)

            inst._connect.assert_called_once()
        inst.session.commit.assert_called_once()

    @patch("gobeventproducer.database.gob.contextmanager.gob_model", spec_set=True)
    def test_get_tables_to_reflect(self, mock_model):
        gdc = GobDatabaseConnection("CAT", "COL", MagicMock())
        mock_model.get_table_name = lambda x, y: f"{x}_{y}".lower()

        expected = [
            "events",
            "cat_col",
            "rel_table_for_some_rel",
            "rel_table_for_some_other_rel",
        ]
        self.assertEqual(expected, gdc._get_tables_to_reflect())

    @patch("gobeventproducer.database.gob.contextmanager.and_")
    def test_get_events(self, mock_and):
        gdc = GobDatabaseConnection("cat", "coll", MagicMock())
        gdc.Event = MagicMock()
        gdc.Event.catalogue = MockComp("catalogue")
        gdc.Event.entity = MockComp("entity")
        gdc.Event.eventid = MockComp("eventid")

        gdc.session = MagicMock()

        res = gdc.get_events(184, 200)

        gdc.session.query.assert_has_calls(
            [
                call(gdc.Event),
                call().yield_per(10000),
                call().yield_per().filter(mock_and.return_value),
                call().yield_per().filter().order_by(gdc.Event.eventid.asc.return_value),
            ]
        )

        self.assertEqual(gdc.session.query().yield_per().filter().order_by(), res)

        mock_and.assert_called_with(
            "catalogue == cat",
            "entity == coll",
            "eventid > 184",
            "eventid <= 200",
        )

        res = gdc.get_events(104, 248, 50)
        gdc.session.query.assert_has_calls(
            [
                call(gdc.Event),
                call().yield_per(10000),
                call().yield_per().filter(mock_and.return_value),
                call().yield_per().filter().order_by(gdc.Event.eventid.asc.return_value),
                call().yield_per().filter().order_by().limit(50)
            ]
        )

        self.assertEqual(gdc.session.query().yield_per().filter().order_by().limit(), res)

        # Check no max set
        gdc.get_events(824)
        mock_and.assert_called_with(
            "catalogue == cat",
            "entity == coll",
            "eventid > 824",
        )


    @patch("gobeventproducer.database.gob.contextmanager.selectinload")
    def test_query_object(self, mock_selectinload):
        gdc = GobDatabaseConnection("cat", "coll", MagicMock())
        gdc.ObjectTable = MagicMock()
        gdc.session = MagicMock()
        gdc.base = MagicMock()

        res = gdc._query_object()

        gdc.session.query.assert_has_calls([
            call(gdc.ObjectTable),
            call().options(mock_selectinload.return_value.selectinload.return_value, mock_selectinload.return_value.selectinload.return_value),
        ])
        self.assertEqual(gdc.session.query().options(), res)

    def test_get_objects(self):
        gdc = GobDatabaseConnection("cat", "coll", MagicMock())
        gdc.ObjectTable = MagicMock()
        gdc.ObjectTable._date_deleted = MockComp("_date_deleted")
        gdc._query_object = MagicMock()

        res = gdc.get_objects()

        gdc._query_object.return_value.filter.assert_called_with("_date_deleted == None")
        gdc._query_object.return_value.filter.return_value.order_by.assert_called_with(gdc.ObjectTable._last_event.asc.return_value)
        gdc._query_object.return_value.filter.return_value.order_by.return_value.yield_per.assert_called_with(5_000)
        self.assertEqual(res, gdc._query_object.return_value.filter.return_value.order_by.return_value.yield_per.return_value)

    def test_get_object(self):
        gdc = GobDatabaseConnection("cat", "coll", MagicMock())
        gdc._query_object = MagicMock()
        gdc.ObjectTable = MagicMock()
        gdc.ObjectTable._tid = MockComp("_tid")

        res = gdc.get_object("24")
        gdc._query_object.return_value.filter.assert_called_with("_tid == 24")
        self.assertEqual(res, gdc._query_object.return_value.filter.return_value.one.return_value)

    @patch("gobeventproducer.database.gob.contextmanager.gob_model", spec_set=True)
    @patch("gobeventproducer.database.gob.contextmanager.MetaData")
    @patch("gobeventproducer.database.gob.contextmanager.create_engine")
    @patch("gobeventproducer.database.gob.contextmanager.Session")
    @patch("gobeventproducer.database.gob.contextmanager.URL")
    @patch("gobeventproducer.database.gob.contextmanager.automap_base")
    @patch("gobeventproducer.database.gob.contextmanager.GOB_DATABASE_CONFIG", {"db": "config"})
    def test_connect(self, mock_automap, mock_url, mock_session, mock_create_engine, mock_metadata, mock_model):
        mock_model.get_table_name = lambda x, y: f"{x}_{y}".lower()

        gdc = GobDatabaseConnection("cat", "coll", MagicMock())
        gdc._get_tables_to_reflect = MagicMock(return_value=["events", "cat_coll"])

        self.assertIsNone(gdc.session)
        self.assertIsNone(gdc.base)
        self.assertIsNone(gdc.Event)
        self.assertIsNone(gdc.ObjectTable)

        gdc._connect()

        # Check initialisation of session
        self.assertEqual(mock_session.return_value, gdc.session)
        mock_session.assert_called_with(mock_create_engine.return_value)
        mock_create_engine.assert_called_with(mock_url.return_value, connect_args={"sslmode": "require"})
        mock_url.assert_called_with(db="config")

        # Check that Event obj is mapped and set correctly
        self.assertEqual(mock_automap.return_value.classes.events, gdc.Event)
        self.assertEqual(mock_automap.return_value.classes.cat_coll, gdc.ObjectTable)
        mock_automap.assert_has_calls(
            [
                call(metadata=mock_metadata.return_value),
                call().prepare(),
            ]
        )
        mock_metadata.assert_has_calls(
            [call(), call().reflect(mock_create_engine.return_value, only=["events", "cat_coll"])]
        )
