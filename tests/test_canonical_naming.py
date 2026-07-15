from qbt_orchestrator.naming import canonical_file_basename, canonical_media_name


def test_canonical_media_name_keeps_id_directory_and_prefixes_title():
    value = canonical_media_name(
        "bban-582",
        "いじられキャラはもっとえっちないじりを期待している",
    )

    assert value.normalized_id == "BBAN-582"
    assert value.metadata_title == "いじられキャラはもっとえっちないじりを期待している"
    assert value.display_title == "BBAN-582 いじられキャラはもっとえっちないじりを期待している"
    assert value.canonical_basename == value.display_title
    assert value.remote_dir("gcrypt:") == "gcrypt:/BBAN-582"


def test_canonical_media_name_sanitizes_only_filesystem_value():
    value = canonical_media_name("ABF-017", '标题 / A:*?"<>|  ')

    assert value.display_title == 'ABF-017 标题 / A:*?"<>|'
    assert value.canonical_basename == "ABF-017 标题 _ A_"


def test_canonical_media_name_never_truncates_id_prefix():
    value = canonical_media_name(
        "FC2-PPV-4684796",
        "名" * 300,
        max_basename_chars=40,
    )

    assert value.canonical_basename.startswith("FC2-PPV-4684796 ")
    assert len(value.canonical_basename) == 40


def test_canonical_media_name_limits_utf8_bytes_for_fuse_mounts():
    value = canonical_media_name("BLK-694", "界" * 200)

    assert value.canonical_basename.startswith("BLK-694 ")
    assert len(value.canonical_basename.encode("utf-8")) <= 220
    assert value.display_title == "BLK-694 " + ("界" * 200)


def test_canonical_file_basename_preserves_multi_part_suffix_and_collision_digest():
    value = canonical_media_name("BBAN-582", "影片名称")

    assert canonical_file_basename(value, "raw-name-CD2.mp4") == "BBAN-582 影片名称-CD2"
    assert (
        canonical_file_basename(
            value,
            "raw-name.mp4",
            collision_digest="A1B2C3D4FFFFFFFF",
        )
        == "BBAN-582 影片名称-a1b2c3d4"
    )
