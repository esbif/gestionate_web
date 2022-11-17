from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_login import LoginManager
from flask_bootstrap import Bootstrap

from config import Config

app = Flask(__name__)
app.config.from_object(Config)
db = SQLAlchemy(app)
migrate = Migrate(app, db)

login_manager = LoginManager(app)
login_manager.login_view = 'auth.login'

boostrap = Bootstrap(app)

from app.main import bp as main_bp
app.register_blueprint(main_bp)

from app.auth import bp as auth_bp
app.register_blueprint(auth_bp)

from app import models
