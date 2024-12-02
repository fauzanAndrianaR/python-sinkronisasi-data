from flask import Flask, render_template, request
import psycopg2
from dotenv import load_dotenv
import os



app = Flask(__name__)

# Konfigurasi database
load_dotenv()

# Konfigurasi database BA
config_BA = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'sslmode': 'require',
    'sslrootcert': 'ca.pem'
}

# Konfigurasi database BB
config_BB = {
    'dbname': os.getenv('DB_BB_NAME'),
    'user': os.getenv('DB_BB_USER'),
    'password': os.getenv('DB_BB_PASSWORD'),
    'host': os.getenv('DB_BB_HOST'),
    'port': os.getenv('DB_BB_PORT'),
    'sslmode': 'require',
    'sslrootcert': 'ca.pem'
}
def fetch_data(query, config):
    try:
        conn = psycopg2.connect(**config)
        cur = conn.cursor()
        cur.execute(query)
        data = cur.fetchall()
        conn.close()
        return data
    except Exception as e:
        print(f"Error: {e}")
        return []

@app.route('/')
def index():
    # Ambil data dari kedua tabel
    query = "SELECT * FROM data_sekolah;"
    data_BA = fetch_data(query, config_BA)
    data_BB = fetch_data(query, config_BB)
    return render_template('index.html', data_BA=data_BA, data_BB=data_BB)

@app.route('/sync', methods=['POST'])
def sync():
    try:
        sync_data()
        return "Sinkronisasi selesai!", 200
    except Exception as e:
        print(f"Error during sync: {e}")
        return "Sinkronisasi gagal!", 500


def sync_data():
    try:
        conn_BA = psycopg2.connect(**config_BA)
        cur_BA = conn_BA.cursor()

        conn_BB = psycopg2.connect(**config_BB)
        cur_BB = conn_BB.cursor()

        # Ambil data dari kedua tabel
        cur_BA.execute("SELECT * FROM data_sekolah;")
        data_BA = {row[0]: row for row in cur_BA.fetchall()}  # Indexed by 'no'

        cur_BB.execute("SELECT * FROM data_sekolah;")
        data_BB = {row[0]: row for row in cur_BB.fetchall()}  # Indexed by 'no'

        # Sinkronisasi dari BA ke BB
        for no, row in data_BA.items():
            if no not in data_BB:
                cur_BB.execute(
                    "INSERT INTO data_sekolah VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);", row
                )
            elif row != data_BB[no]:
                cur_BB.execute(
                    """
                    UPDATE data_sekolah
                    SET kabupaten_kota = %s, negeri = %s, swasta = %s, jml = %s,
                        n1 = %s, s1 = %s, sub_jml1 = %s,
                        n2 = %s, s2 = %s, sub_jml2 = %s,
                        n3 = %s, s3 = %s, sub_jml3 = %s
                    WHERE no = %s;
                    """,
                    row[1:] + (row[0],)
                )

        # Sinkronisasi dari BB ke BA
        for no, row in data_BB.items():
            if no not in data_BA:
                cur_BA.execute(
                    "INSERT INTO data_sekolah VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);", row
                )
            elif row != data_BA[no]:
                cur_BA.execute(
                    """
                    UPDATE data_sekolah
                    SET kabupaten_kota = %s, negeri = %s, swasta = %s, jml = %s,
                        n1 = %s, s1 = %s, sub_jml1 = %s,
                        n2 = %s, s2 = %s, sub_jml2 = %s,
                        n3 = %s, s3 = %s, sub_jml3 = %s
                    WHERE no = %s;
                    """,
                    row[1:] + (row[0],)
                )

        conn_BA.commit()
        conn_BB.commit()
    except Exception as e:
        print(f"Terjadi kesalahan: {e}")
    finally:
        if conn_BA:
            cur_BA.close()
            conn_BA.close()
        if conn_BB:
            cur_BB.close()
            conn_BB.close()

if __name__ == "__main__":
    app.run(debug=True)
