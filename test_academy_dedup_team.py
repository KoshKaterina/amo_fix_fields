"""Сторож дублей Академии (ops/academy-dedup): кому достаётся склеенная сделка.

Кирилл Полесский с 21.10.2026 получает половину новых сделок из GetCourse. Без него в
команде склейка его сделки с сделкой бота отдавала результат кому-то из троих прежних."""

import importlib.util
import pathlib
import sys
import types

import pytest

APP = pathlib.Path(__file__).resolve().parent / "ops" / "academy-dedup" / "app"


@pytest.fixture(scope="module")
def bp():
    saved = sys.modules.get("amo")
    sys.modules["amo"] = types.ModuleType("amo")          # сеть сторожу в тесте не нужна
    try:
        spec = importlib.util.spec_from_file_location("academy_dedup_build_plan", APP / "build_plan.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        yield module
    finally:
        if saved is None:
            sys.modules.pop("amo", None)
        else:
            sys.modules["amo"] = saved


def _group(*owners):
    return [{"id": i, "responsible_user_id": uid} for i, uid in enumerate(owners)]


def test_kirill_is_in_team(bp):
    assert 13946318 in bp.ACADEMY_TEAM and len(bp.ACADEMY_TEAM) == 4


def test_kirill_keeps_his_deal_when_merged_with_service_owner(bp):
    assert bp.choose_responsible(_group(13946318, 11513202), "p")[0] == 13946318


def test_artem_rule_unchanged(bp):
    assert bp.choose_responsible(_group(13946318, 13822630), "p")[0] == 13822630


def test_two_managers_pick_one_of_them(bp):
    owner, _ = bp.choose_responsible(_group(13946318, 14289086), "p")
    assert owner in (13946318, 14289086)
