import datetime
import unittest
from unittest.mock import AsyncMock, patch

import academy_consent_stamp as mod


class AcademyConsentStampTest(unittest.IsolatedAsyncioTestCase):
    async def test_stamps_both_consents_for_new_academy_lead(self):
        contact = {
            "custom_fields_values": [
                {"field_id": mod.FIELD_ACADEMY_PD_CONSENT, "values": [{"value": "да"}]},
                {"field_id": mod.FIELD_ACADEMY_MARKETING_CONSENT, "values": [{"value": "да"}]},
            ],
            "_embedded": {"leads": [{"id": 10}]},
        }
        lead = {
            "id": 10,
            "pipeline_id": mod.PIPELINE_ACADEMY,
            "created_at": mod.ACADEMY_CUTOVER_TS,
        }
        now = datetime.datetime(2026, 9, 24, 18, 47, tzinfo=datetime.timezone(datetime.timedelta(hours=3)))
        with (
            patch.object(mod.amo_service, "get_contact_by_id", AsyncMock(return_value=contact)),
            patch.object(mod.amo_service, "get_lead_full", AsyncMock(return_value=lead)),
            patch.object(mod.amo_service, "patch_contact", AsyncMock(return_value={"ok": True})) as write,
        ):
            result = await mod.process(5, set(mod._TARGETS), now=now)
        self.assertEqual(result, "stamped")
        write.assert_awaited_once_with(5, custom_fields={
            mod.FIELD_ACADEMY_PD_DATE_TEXT: "24/09/2026",
            mod.FIELD_ACADEMY_MARKETING_DATE_TEXT: "24/09/2026",
        })

    async def test_skips_historical_lead(self):
        contact = {
            "custom_fields_values": [
                {"field_id": mod.FIELD_ACADEMY_PD_CONSENT, "values": [{"value": "да"}]},
            ],
            "_embedded": {"leads": [{"id": 10}]},
        }
        lead = {
            "id": 10,
            "pipeline_id": mod.PIPELINE_ACADEMY,
            "created_at": mod.ACADEMY_CUTOVER_TS - 1,
        }
        with (
            patch.object(mod.amo_service, "get_contact_by_id", AsyncMock(return_value=contact)),
            patch.object(mod.amo_service, "get_lead_full", AsyncMock(return_value=lead)),
            patch.object(mod.amo_service, "patch_contact", AsyncMock()) as write,
        ):
            result = await mod.process(5, {mod.FIELD_ACADEMY_PD_CONSENT})
        self.assertEqual(result, "no_new_academy_lead")
        write.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
