from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileRequired
from wtforms import SubmitField
from wtforms.validators import DataRequired

from werkzeug.utils import secure_filename

class DataForm(FlaskForm):
    op_file = FileField('Operativos', validators=[FileRequired()])
    non_op_file = FileField('No operativos', validators=[FileRequired()])
    submit = SubmitField('Upload')
