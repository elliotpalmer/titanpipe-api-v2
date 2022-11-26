from api import app

if __name__ == "__main__":
    from api.db import db
    db.init_app(app)
    app.run(host="0.0.0.0", port=8080, debug=True)