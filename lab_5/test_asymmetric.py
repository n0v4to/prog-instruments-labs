import pytest
import os
import tempfile


from asymmetric import Asymmetric


def test_generate_keys():
    asymmetric = Asymmetric()
    asymmetric.generate_keys()
    assert asymmetric.private_key is not None
    assert asymmetric.public_key is not None


def test_serialization_public():
    asymmetric = Asymmetric()
    asymmetric.generate_keys()

    with tempfile.NamedTemporaryFile(delete=False) as public_file:
        public_path = public_file.name
        asymmetric.serialization_public(public_path)

    assert os.path.exists(public_path)
    os.remove(public_path)  # Удаляем файл после теста


def test_serialization_private():
    asymmetric = Asymmetric()
    asymmetric.generate_keys()

    with tempfile.NamedTemporaryFile(delete=False) as private_file:
        private_path = private_file.name
        asymmetric.serialization_private(private_path)

    assert os.path.exists(private_path)
    os.remove(private_path)  # Удаляем файл после теста


def test_public_key_deserialization():
    asymmetric = Asymmetric()
    asymmetric.generate_keys()
    with tempfile.NamedTemporaryFile(delete=False) as public_file:
        public_path = public_file.name
        asymmetric.serialization_public(public_path)

    new_asymmetric = Asymmetric()
    new_asymmetric.public_key_deserialization(public_path)

    assert new_asymmetric.public_key is not None
    os.remove(public_path)  # Удаляем файл после теста


def test_private_key_deserialization():
    asymmetric = Asymmetric()
    asymmetric.generate_keys()
    with tempfile.NamedTemporaryFile(delete=False) as private_file:
        private_path = private_file.name
        asymmetric.serialization_private(private_path)

    new_asymmetric = Asymmetric()
    new_asymmetric.private_key_deserialization(private_path)

    assert new_asymmetric.private_key is not None
    os.remove(private_path)  # Удаляем файл после теста


@pytest.mark.parametrize("symmetric_key", [
    b"shortkey",
    b"this_is_a_very_long_symmetric_key_for_testing_purposes",
    b"1234567890"
])
def test_encrypt_decrypt(symmetric_key):
    asymmetric = Asymmetric()
    asymmetric.generate_keys()

    encrypted = asymmetric.encrypt(symmetric_key)
    decrypted = asymmetric.decrypt(encrypted)

    assert symmetric_key == decrypted


@pytest.mark.parametrize("method_name", [
    "serialization_public",
    "serialization_private",
    "public_key_deserialization",
    "private_key_deserialization"
])
def test_file_not_found_error_handling(mocker, method_name, capfd):
    asymmetric = Asymmetric()
    asymmetric.generate_keys()

    mocker.patch("builtins.open", side_effect=FileNotFoundError)

    method = getattr(asymmetric, method_name)
    method("non_existent_file")

    captured = capfd.readouterr()  # Считываем вывод
    assert "Ошибка! Файл non_existent_file не найден." in captured.out
