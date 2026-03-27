from pipeline import filter_companies_for_shard, shard_for_company, should_mark_removed


def test_shard_for_company_is_stable():
    shard_a = shard_for_company("greenhouse", "anthropic", 8)
    shard_b = shard_for_company("greenhouse", "anthropic", 8)
    assert shard_a == shard_b


def test_filter_companies_for_shard_covers_all_companies_once():
    companies = [
        ("greenhouse", "anthropic"),
        ("greenhouse", "figma"),
        ("lever", "spotify"),
        ("ashby", "ramp"),
        ("jobvite", "logitech"),
    ]
    shards = []
    for shard_index in range(4):
        shards.extend(filter_companies_for_shard(companies, shard_index, 4))

    assert sorted(shards) == sorted(companies)
    assert len(shards) == len(companies)


def test_filter_companies_for_shard_noop_without_shard_args():
    companies = [("greenhouse", "anthropic"), ("lever", "spotify")]
    assert filter_companies_for_shard(companies, None, None) == companies


def test_should_mark_removed_only_for_complete_scrapes():
    assert should_mark_removed(3, None) is True
    assert should_mark_removed(3, 10) is True
    assert should_mark_removed(10, 10) is False
