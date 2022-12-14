from datetime import datetime
from werkzeug.utils import secure_filename
import pandas as pd
import urllib.parse
import os

from app.main import bp
from flask import render_template, url_for, session, flash, redirect
from flask import session, current_app, send_file
from flask_login import login_required, current_user
from app.main.forms import DataForm, FilterForm, ComplianceForm
from app.main.reporter import build_reporter_from_gestionate
from app.main.reporter import get_compliance_report
from app.main.grapher import Grapher
from app import db

@bp.route('/', methods=['POST','GET'])
@bp.route('/index', methods=['POST','GET'])
@login_required
def index():

    if "data_pkl_path" in session.keys():

        test_data = pd.read_pickle(session["data_pkl_path"])
        sites = pd.read_pickle("data_sets/locations.pkl")

        reporter = build_reporter_from_gestionate(test_data)
        reporter.filter_sites(sites)
        reporter.eval_tests()

        if "filters" in session.keys():
            reporter.apply_filters(session["filters"])

        summary = reporter.get_summary()

        failed_prog = reporter.get_failed_by_day_hr()
        Grapher.progress_stacked_area(failed_prog, "failed.png")
        
        plt_data_dn = pd.DataFrame()
        plt_data_dn["x"] = reporter.evaluated["timestamp"]
        plt_data_dn["y"] = reporter.evaluated["dn_br"]
        plt_data_dn["pass"] = reporter.evaluated["dn_pass"]
        Grapher.progress_scatter(plt_data_dn, "dn_br.png", 0.75)

        plt_data_up = pd.DataFrame()
        plt_data_up["x"] = reporter.evaluated["timestamp"]
        plt_data_up["y"] = reporter.evaluated["up_br"]
        plt_data_up["pass"] = reporter.evaluated["up_pass"]
        Grapher.progress_scatter(plt_data_up, "up_br.png", 0.75)

        return render_template(
                'index.html', title='Home', summary=summary)

    return render_template('index.html', title='Home')

@bp.route('/filters', methods=['POST','GET'])
@login_required
def filters():
    form = FilterForm()
    if form.validate_on_submit():
        session["filters"] = {}
        session["filters"].update({"profile_id": form.profile.data})
        session["filters"].update({"type": form.test_type.data})
        return redirect(url_for("main.index"))
    return render_template(
            'filters.html', title='Filters', form=form)

@bp.route('/data_loader', methods=['POST','GET'])
@login_required
def data_loader():
    form = DataForm()
    if form.validate_on_submit():

        timestamp = datetime.strftime(datetime.now(), "%d%m%y_%H%M%S")

        op_filename = secure_filename("op_" + timestamp + ".xlsx")
        non_op_filename = secure_filename("non_op_" + timestamp + ".xlsx")

        form.op_file.data.save("uploads/" + op_filename)
        form.non_op_file.data.save("uploads/" + non_op_filename)

        op_data = pd.read_excel(
                "uploads/" + op_filename, sheet_name = "ReportSheet",
                header=1, parse_dates=True)
        non_op_data = pd.read_excel(
                "uploads/" + non_op_filename, sheet_name = "ReportSheet",
                header=1, parse_dates=True)

        test_data = pd.concat([op_data, non_op_data],
                ignore_index = True, axis=0)
        test_data.reset_index()

        pkl_path = "data_sets/" + timestamp + ".pkl"
        test_data.to_pickle(pkl_path)
        session["data_pkl_path"] =  pkl_path

        return redirect(url_for('main.index'))

    return render_template(
            'data_loader.html', title='Data Loader', form=form)

@bp.route('/progress', methods=["GET"])
@login_required
def progress():
    if "data_pkl_path" in session.keys():

        # Load data.
        test_data = pd.read_pickle(session["data_pkl_path"])
        sites = pd.read_pickle("data_sets/locations.pkl")

        # Init reporter.
        reporter = build_reporter_from_gestionate(test_data)
        reporter.filter_sites(sites)
        reporter.eval_tests()
        if "filters" in session.keys():
            reporter.apply_filters(session["filters"])

        # Get relevant data.
        progress = reporter.get_progress()

        # Add links.
        url = url_for("main.day")
        link = '<a href="'+url+'/{0}">{0}</a>'
        progress["timestamp"] = progress["timestamp"].dt.date.apply(
                lambda x: link.format(urllib.parse.quote_plus(
                        x.strftime("%Y-%m-%d"))))
        return render_template(
                "table.html", title="Progress", data=progress)
    return redirect(url_for("main.index"))

@bp.route("/compliance", methods=["GET", "POST"])
@login_required
def compliance():
    form = ComplianceForm()
    if form.validate_on_submit():
        
        timestamp = datetime.strftime(datetime.now(), "%d%m%y_%H%M%S")

        op_filename = secure_filename("C_op_" + timestamp + ".xlsx")
        non_op_filename = secure_filename(
                "C_non_op_" + timestamp + ".xlsx")
        tickets_filename = secure_filename(
                "C_loc_" + timestamp + ".xlsx")

        form.op_file.data.save("uploads/" + op_filename)
        form.non_op_file.data.save("uploads/" + non_op_filename)
        form.tickets_file.data.save("uploads/" + tickets_filename)

        op_data = pd.read_excel(
                "uploads/" + op_filename, sheet_name=0,
                header=1, parse_dates=True)
        non_op_data = pd.read_excel(
                "uploads/" + non_op_filename, sheet_name=0,
                header=1, parse_dates=True)
        tickets_data = pd.read_excel(
                "uploads/" + tickets_filename , sheet_name=0,
                header=0, parse_dates=True)

        test_data = pd.concat([op_data, non_op_data],
                ignore_index = True, axis=0)
        test_data.reset_index()

        pkl_path = "data_sets/" + timestamp + ".pkl"
        test_data.to_pickle(pkl_path)
        sites = pd.read_pickle("data_sets/locations.pkl")
        reporter = build_reporter_from_gestionate(test_data)
        reporter.filter_sites(sites)
        print("################## esto es despues de filtrar locations")
        print(reporter.filtered)

        filtered_data = reporter.filtered[
                reporter.filtered["res"]=="succeeded"]
        print("################## esto es despues de filtrar succeeded")
        print(filtered_data)

        remove_list = form.remove_vsats.data.split(",")
        for item in remove_list:
            filtered_data = filtered_data[
                filtered_data["site"] != item.strip()]
        print("################## esto es despues de filtrar rmv ids")
        print(filtered_data)

        type_filter = []
        if form.scheduled.data:
            type_filter.append("scheduled")
        if form.on_demand.data:
            type_filter.append("on-demand")
        if form.monitoring.data:
            type_filter.append("monitoring")

        filtered_data = filtered_data[
                filtered_data["type"].isin(type_filter)]
        print("########## esto es despues de filtrar type")
        print(filtered_data)

        cmpl_file_path = get_compliance_report(filtered_data, tickets_data)

        return send_file(cmpl_file_path, as_attachment=True)
        #return redirect(url_for("main.index"))

    return render_template(
            "compliance.html", title="Compliance", form=form)

@bp.route('/vsat', methods=["GET"])
@bp.route('/vsat/<vsat_id>', methods=["GET"])
@login_required
def vsat(vsat_id=None):
    if "data_pkl_path" in session.keys():
            
        # Load data.
        test_data = pd.read_pickle(session["data_pkl_path"])
        sites = pd.read_pickle("data_sets/locations.pkl")

        # Init reporter.
        reporter = build_reporter_from_gestionate(test_data)
        reporter.filter_sites(sites)
        reporter.eval_tests()
        if "filters" in session.keys():
            reporter.apply_filters(session["filters"])

        if vsat_id:
            data = reporter.evaluated[
                    reporter.evaluated["site"]==vsat_id]

            plt_data_dn = pd.DataFrame()
            plt_data_dn["x"] = data["timestamp"]
            plt_data_dn["y"] = data["dn_br"]
            plt_data_dn["pass"] = data["dn_pass"]
            Grapher.progress_scatter(plt_data_dn, "vsat_dn_br.png",
                    dot_size=12)

            plt_data_up = pd.DataFrame()
            plt_data_up["x"] = data["timestamp"]
            plt_data_up["y"] = data["up_br"]
            plt_data_up["pass"] = data["up_pass"]
            Grapher.progress_scatter(plt_data_up, "vsat_up_br.png",
                    dot_size=12)
            return render_template("vsat_graph.html")

        vsats = reporter.get_vsats()

        url = url_for("main.vsat")
        link = '<a href="'+url+'/{0}">{0}</a>'
        vsats["site"] = vsats["site"].apply(
                lambda x: link.format(urllib.parse.quote_plus(x)))

        return render_template(
                "table.html", title="VSAT", data=vsats)
    return redirect(url_for("main.index"))

@bp.route('/day', methods=["GET"])
@bp.route('/day/<date>', methods=["GET"])
@login_required
def day(date):
    if "data_pkl_path" in session.keys():
        # Load data.
        test_data = pd.read_pickle(session["data_pkl_path"])
        sites = pd.read_pickle("data_sets/locations.pkl")

        # Init reporter.
        reporter = build_reporter_from_gestionate(test_data)
        reporter.filter_sites(sites)
        reporter.eval_tests()
        if "filters" in session.keys():
            reporter.apply_filters(session["filters"])
        
        selected_date = datetime.strptime(date, "%Y-%m-%d")
        data = reporter.evaluated[
                reporter.evaluated[
                        "timestamp"].dt.date==selected_date.date()]

        plt_data_dn = pd.DataFrame()
        plt_data_dn["x"] = data["timestamp"]
        plt_data_dn["y"] = data["dn_br"]
        plt_data_dn["pass"] = data["dn_pass"]
        Grapher.progress_scatter(plt_data_dn, "day_dn_br.png")

        plt_data_up = pd.DataFrame()
        plt_data_up["x"] = data["timestamp"]
        plt_data_up["y"] = data["up_br"]
        plt_data_up["pass"] = data["up_pass"]
        Grapher.progress_scatter(plt_data_up, "day_up_br.png")
        return render_template("day_graph.html")

    return redirect(url_for("main.index"))

