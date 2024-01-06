import base64
import pandas as pd
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import serialization, hashes


def rsa_keys(path):
    """
    Function use to generate private key and public key.
    This function uses RSA algorithm. Public key can be used
    to encrypt data and private key can be used to decrypt data.
    Parameters
    ----------
    @param path: str,
        A valid string specifying the path where keys should be generated
    """

    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=4096,
        backend=default_backend()
    )
    public_key = private_key.public_key()


    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    with open(path + 'keys/rsa_private_key.pem', 'wb') as f:
        f.write(private_pem)

    public_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )
    with open(path + 'keys/rsa_public_key.pem', 'wb') as f:
        f.write(public_pem)


def encryption(path, plaintext):
    """
    Function use to encrypt plain text using pre-generated
    public key.
    Parameters
    ----------
    @param path: str,
        path where public key is saved.
    @param plaintext: str
        plain text which need to be encrypted.
        
    Returns
    -------
    @return encrypted cipertext.
    """
    with open(path, "rb") as key_file:
        public_key = serialization.load_pem_public_key(
            key_file.read(),
            backend=default_backend()
        )

    encrypted = base64.b64encode(public_key.encrypt(
    plaintext,
    padding.OAEP(
        mgf=padding.MGF1(algorithm=hashes.SHA256()),
        algorithm=hashes.SHA256(),
        label=None
        )
    ))

    return encrypted


def decryption(path, ciphertext):
    """
    Function use to decrypt cipertext using pre-generated
    private key.
    Parameters
    ----------
    @param path: str,
        path where private key is saved.
    @param ciphertext: str
        ciphertext which need to be decrypted.
        
    Returns
    -------
    @return decrypted plain text.
    """
    with open(path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )

    decrypted = private_key.decrypt(
    base64.b64decode(ciphertext),
    padding.OAEP(
        mgf=padding.MGF1(algorithm=hashes.SHA256()),
        algorithm=hashes.SHA256(),
        label=None
        )
    )

    return decrypted


def crypto_data(readsql, col, key, conn, insertquery, method):
    """
    Function use to encrypt or decrypt a specific
    column of a table.
    Parameters
    ----------
    @param readsql: str,
        sql file path which use to read data.
    @param col: str
        column of a table which needs to be encrypted or decrypted.
    @param key: str
        public or private key.
    @param conn: str
        database connection string.
    @param insertquery: str
        query which use to insert data.
    @param method: str
        whether it is encryption or decryption method.
    """
    cursor = conn.cursor()
    script_dir = os.path.dirname(__file__)
    abs_file_path = os.path.join(script_dir, readsql)
    query = open(abs_file_path, 'r').read()

    key_file_path = os.path.join(script_dir, key)

    df = pd.read_sql(query, conn)
    df[col] = df[col].apply(lambda x: method(key_file_path, x.encode()).decode())

    for index, row in df.iterrows():
        cursor.execute(insertquery, tuple(row))

    conn.commit()
    cursor.close()