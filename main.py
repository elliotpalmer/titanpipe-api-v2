from flask_json import FlaskJSON
from api import app 
from api.db import db
import os
db.init_app(app)
json = FlaskJSON(app)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=os.getenv("PORT", default=5000), debug=True)