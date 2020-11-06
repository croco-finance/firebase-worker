from flask import Flask
from flask_cors import CORS

app = Flask(__name__)
CORS(app)


@app.route('/api/v1/snaps/<string:address>/')
def get_snaps(address):
    return True


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=80)
