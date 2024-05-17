from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/callback')
def callback():
    code = request.args.get('code', None)
    error = request.args.get('error', None)
    if code:
        return jsonify({"Success": "Authorization code received", "Code": code}), 200
    if error:
        return jsonify({"Error": "Authorization process failed", "Details": error}), 400
    return "No code or error provided by Spotify.", 400

if __name__ == '__main__':
    app.run(port=8888, debug=True)