from __future__ import annotations

import unittest

from app import recommendation_progress


class RecommendationProgressTests(unittest.TestCase):
    def test_tracks_processed_people_and_completion(self) -> None:
        recommendation_progress.start("job-test", 3)
        recommendation_progress.advance("job-test")
        current = recommendation_progress.get("job-test")
        self.assertEqual(
            {key:current[key] for key in ("found","processed","total","complete")},
            {"found":True, "processed":1, "total":3, "complete":False},
        )
        recommendation_progress.set_stage("job-test", "details", "API · карточки", 2, "объектов")
        recommendation_progress.advance("job-test", "details")
        recommendation_progress.add_warning("job-test", "API", "HTTP 500")
        current = recommendation_progress.get("job-test")
        self.assertEqual([stage["id"] for stage in current["stages"]], ["people","details"])
        self.assertEqual(current["stages"][1]["processed"], 1)
        self.assertEqual(current["warnings"], [{"provider":"API","message":"HTTP 500"}])
        recommendation_progress.finish("job-test")
        self.assertEqual(recommendation_progress.get("job-test")["processed"], 2)
        self.assertTrue(recommendation_progress.get("job-test")["complete"])
