from api import app
import os

if __name__ == "__main__":
    from api.db import db
    db.init_app(app)
    app.run(host="0.0.0.0", port=os.getenv("PORT", default=5000), debug=True)