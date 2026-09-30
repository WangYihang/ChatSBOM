from chatsbom.models.language import Language


def test_language_enum_values():
    assert Language.GO == 'go'
    assert Language.PYTHON == 'python'
    assert str(Language.GO) == 'go'
