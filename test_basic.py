import unittest

import datetime

from searchable_cipher import SearchableCipher


class TestEncryption(unittest.TestCase):
    sc = SearchableCipher(b'key goes here!!!')

    def test_encrypt_tokens(self):
        tokens = ["foo", "bar"]
        ciphers = self.sc.encrypt_tokens(tokens)

        self.assertNotEqual(ciphers, tokens)
        self.assertEqual(self.sc.decrypt_tokens(ciphers), tokens)

    def test_encrypt_tokenize_text(self):
        tokens = ["foo", "bar"]
        ciphers = self.sc.encrypt_tokenize_text("Foo BAR!")

        self.assertEqual(self.sc.decrypt_tokens(ciphers), tokens)

    def test_encrypt_string(self):
        string = "The quick brown fox jumps over the lazy dog!"
        cipher = self.sc.encrypt_token(string)

        self.assertNotEqual(cipher, string)
        self.assertEqual(self.sc.decrypt_token(cipher), string)

    def test_encrypt_int(self):
        number = 12345
        cipher = self.sc.encrypt_int(number)

        self.assertNotEqual(cipher, number)
        self.assertEqual(self.sc.decrypt_int(cipher), number)

    def test_encrypt_date(self):
        date = datetime.date(2010, 11, 12)
        cipher = self.sc.encrypt_date(date)

        self.assertNotEqual(cipher, date)
        self.assertEqual(self.sc.decrypt_date(cipher), date)

    def test_encrypt_time(self):
        time = datetime.time(13, 14, 15)
        cipher = self.sc.encrypt_time(time)

        self.assertNotEqual(cipher, time)
        self.assertEqual(self.sc.decrypt_time(cipher), time)


if __name__ == '__main__':
    unittest.main()
