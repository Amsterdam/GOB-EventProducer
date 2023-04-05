from unittest import TestCase
from unittest.mock import MagicMock, call, patch

from gobeventproducer.database.gob.contextmanager import GobDatabaseConnection


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


class TestGobDatabaseConnection(TestCase):
    def test_context_manager(self):
        inst = GobDatabaseConnection("", "", MagicMock())
        inst._connect = MagicMock()
        inst.session = MagicMock()

        with inst as tst:
            self.assertEqual(tst, inst)

            inst._connect.assert_called_once()
        inst.session.commit.assert_called_once()

    @patch("gobeventproducer.database.gob.contextmanager.get_relations_for_collection")
    @patch("gobeventproducer.database.gob.contextmanager.gob_model", spec_set=True)
    def test_get_tables_to_reflect(self, mock_model, mock_get_relations):
        mock_get_relations.return_value = {"rel a": "a", "rel b": "b"}
        gdc = GobDatabaseConnection("CAT", "COL", MagicMock())
        mock_model.get_table_name = lambda x, y: f"{x}_{y}".lower()

        expected = [
            "events",
            "cat_col",
            "rel_a",
            "rel_b",
        ]
        self.assertEqual(expected, gdc._get_tables_to_reflect())
        mock_get_relations.assert_called_with(mock_model, "CAT", "COL")

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

    def test_get_objects(self):
        gdc = GobDatabaseConnection("cat", "coll", MagicMock())
        gdc.session = MagicMock()

        res = gdc.get_objects()

        gdc.session.query.assert_has_calls([
            call(gdc.ObjectTable),
            call().yield_per(10_000),
        ])
        self.assertEqual(gdc.session.query().yield_per(), res)

    def test_get_object(self):
        gdc = GobDatabaseConnection("cat", "coll", MagicMock())
        gdc.ObjectTable = MagicMock()
        gdc.ObjectTable._tid = MockComp("_tid")
        gdc.session = MagicMock()

        res = gdc.get_object("24")

        gdc.session.query.assert_has_calls([
            call(gdc.ObjectTable),
            call().filter("_tid == 24"),
        ])
        self.assertEqual(gdc.session.query().filter().one(), res)

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
