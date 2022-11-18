from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileRequired
from wtforms import SubmitField, SelectField, BooleanField, TextAreaField
from wtforms.validators import DataRequired, Length

from werkzeug.utils import secure_filename

class DataForm(FlaskForm):
    op_file = FileField('Operativos', validators=[FileRequired()])
    non_op_file = FileField('No operativos', validators=[FileRequired()])
    submit = SubmitField('Upload')

class FilterForm(FlaskForm):
    profile = SelectField('Perfil', validators=[DataRequired()],
            choices=[(12, '12x3'),(15, '15x3.75'),(18, '18x4.5'),
                (21, '21x5.25'), (None, 'All')])
    submit = SubmitField('Filter')

class ComplianceForm(FlaskForm):
    op_file = FileField('Operativos', validators=[FileRequired()])
    non_op_file = FileField('No operativos', validators=[FileRequired()])
    tickets_file = FileField('Tickets', validators=[FileRequired()])
    scheduled = BooleanField('Scheduled')
    on_demand = BooleanField('On Demand')
    monitoring = BooleanField('Monitoring')
    remove_vsats = TextAreaField(
            'Remove IDs', validators=[Length(min=0, max=500)])
    submit = SubmitField('Calculate')
