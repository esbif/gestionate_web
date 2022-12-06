EXCEL_KEY_MAPPING = {
        "Ubicación": "site",
        "BW Bajada Esperado": "exp_dn_br",
        "BW Bajada Encontrado": "dn_br",
        "BW Subida Esperado": "exp_up_br",
        "BW Subida Encontrado": "up_br",
        "Resultado": "res",
        "Fecha de la Prueba": "timestamp",
        "Hora de la Prueba": "hour",
        "Perfil de Velocidad": "profile",
        "Tipo de prueba": "type",
        "Tipo de Prueba": "type",
        "Error": "error"}

import pandas as pd
import numpy as np
from datetime import datetime
from werkzeug.utils import secure_filename
from flask import current_app
import os

class Reporter:

    def __init__(self, data):
        self.test_data = data

    def filter_sites(self, sites):
        """ Remove all tests from sites not included in "sites" var."""

        sites = sites.drop_duplicates()

        # Remove unused data from site id. "Locations" only includes the 
        # location bit in the site id. 
        data = self.test_data
        data["clean_loc"] = data.apply(
                lambda row: int(row.site.split("-")[0]), axis=1)

        filtered = data[data["clean_loc"].isin(sites)]

        self.filtered = filtered.drop(columns=["clean_loc"])
        return self.filtered

    def apply_filters(self, filters):
        df1 = self.evaluated
        df2 = self.filtered
        for key, value in filters.items():
            if value == "None":
                continue
            try:
                value = int(value)
            except Exception as e:
                value = value
            df1 = df1[df1[key]==value]
            try:
                df2 = df2[df2[key]==value]
            except Exception as e:
                df2 = df2
        self.evaluated = df1
        self.filtered = df2
        return self.evaluated
            
    def get_summary(self):
        res = {}
        res.update({
            "size": self.filtered.size,
            "start": self.filtered["timestamp"].min().date(),
            "end": self.filtered["timestamp"].max().date(),
            "failed_qty": self.filtered[
                    self.filtered["res"] == "failed"].size,
            "succeeded_qty": self.filtered[
                    self.filtered["res"] == "succeeded"].size})
        return res

    def get_failed(self):
        failed = self.filtered[self.filtered["res"] == "failed"]
        failed["err"] = failed.apply(
                lambda row: (row.error[:40] + '..') if len(
                        row.error) > 75 else row.error, axis=1) 
        df = failed["err"].value_counts()
        df = df.rename_axis("err").to_frame("count")
        top_five = df.sort_values("count", ascending=False)[:5].copy()
        top_five.loc["others"] = df["count"][5:].sum()
        return top_five 

    def get_failed_by_day_hr(self):
        """ This function counts the number of errors of each time
        and groups them by the hour """

        failed = self.filtered[self.filtered["res"] == "failed"]

        failed["err"] = failed.apply(
                lambda row: (row.error[:40] + '..') if len(
                        row.error) > 75 else row.error, axis=1) 
        res = failed.set_index(
                "timestamp").resample('H')['err'].value_counts().unstack()

        return res

    def get_succeeded(self):
        df = self.evaluated["pass"].value_counts()
        df = df.rename_axis("pass").to_frame("count")
        print(df)
        return df

    def eval_tests(self):
        """ Check if test is passed or failed on dowload, upload and both
        directions. Add columns with this information to data."""

        def eval_func(row, direction):
            expected = getattr(row, "exp_"+direction+"_br")
            found = getattr(row, direction+"_br")
            if expected > found:
                return False 
            return True

        # Use eval_func to check tests.
        data = self.filtered[self.filtered["res"] == "succeeded"]

        data["dn_pass"] = data.apply(
                lambda row: eval_func(row, "dn"), axis=1)
        data["up_pass"] = data.apply(
                lambda row: eval_func(row, "up"), axis=1)

        data["pass"] = data.dn_pass & data.up_pass

        self.evaluated = data

        return data

    def get_progress(self):
        df = self.evaluated

        def f(x):
            d = {}
            d["count"] = len(x["site"])
            d["dn_fail"] = 100*len(x[x["dn_pass"]==False])/len(x["site"])
            d["up_fail"] = 100*len(x[x["up_pass"]==False])/len(x["site"])
            return pd.Series(d)
        res = df.set_index(
                "timestamp").resample("D").apply(f).reset_index()
        return(res)

    def get_vsats(self):

        df = self.evaluated

        def f(x):
            d = {}
            d["profile"] = x["profile"].unique()[0]
            d["count"] = len(x["site"])
            d["dn_fail"] = 100*len(x[x["dn_pass"]==False])/len(x["site"])
            d["up_fail"] = 100*len(x[x["up_pass"]==False])/len(x["site"])
            return pd.Series(d)
        res = df.groupby("site").apply(f).reset_index().sort_values(
                "up_fail", ascending=False)
        return(res)

def build_reporter_from_gestionate(data):
    """ Rename columns and remove unused ones."""

    clean = pd.DataFrame()
    for key, value in EXCEL_KEY_MAPPING.items():
        clean[value] = data[key]

    # Parse timestamp column as datetime. 
    clean["timestamp"] = pd.to_datetime(clean["timestamp"],
            format="%Y-%m-%d %H:%M:%S.%f")

    # Remove duplicate tests.
    clean.drop_duplicates(
            ['site', 'dn_br', 'up_br', 'timestamp', 'hour'])

    def profile_id(row):
        profile = row["profile"]
        profile_id = int(
                profile.split("-")[0].split(":")[1].split(".")[0].strip())
        return profile_id

    clean["profile_id"] = clean.apply(lambda x: profile_id(x), axis=1)
    reporter = Reporter(clean)
    return reporter 

########################################################################
########################################################################
########################################################################

def filter_test_qty_tkt(tests, tickets_tmp, min_test_qty, tkt_test_qty):
    """ Remove tests from sites with less than "min_test_qty" tests. 
    In case site has a ticket, use "tkt_test_qty". """

    # Check interruption size. 
    tickets = pd.DataFrame()
    tickets['site'] = tickets_tmp['ID_BENEFICIARIO']
    tickets['start'] = pd.to_datetime(
            tickets_tmp['FECHA_HORA_DE_APERTURA'])
    tickets['end'] = pd.to_datetime(
            tickets_tmp['FECHA_HORA_DE_RESOLUCION'])
    tickets['end'] = tickets['end'].fillna(datetime.today())
    tickets['dn_time'] = (tickets['end'] - tickets['start'])
    tickets['dn_time_days'] = tickets['dn_time'].dt.days

    # Check total downtime per site.
    tickets = tickets.groupby(by=['site']).sum()
    tickets = tickets.reset_index()

    # Check total test qty per site. 
    test_qty = tests.groupby("site").size().reset_index(name="count")
    test_qty["site"] = test_qty.apply(
            lambda row: int(row.site.split("-")[0]), axis=1)

    # Put number of tests and total downtime in one table.
    summary = pd.merge(test_qty, tickets, on="site", how="left")
    summary['dn_time_days'] = summary['dn_time_days'].fillna(0)

    profiles = pd.DataFrame()
    profiles["site"] = tests["site"]
    profiles["profile"] = tests["profile"]
    profiles = profiles.drop_duplicates(['site', "profile"])
    profiles["site"] = profiles.apply(
            lambda row: int(row.site.split("-")[0]), axis=1)

    summary = pd.merge(summary, profiles, on="site", how="left")

    # Check valid tests based on contracts logic: a site must have 
    # 30 or more tests unless it has been out for 24 hrs or more 
    # (tickets). In that case it should have at least 15 tests.
    def check_valid_sites(row):
        if row["count"] < 30:
            if row["dn_time_days"] >= 1:
                if row["count"] < 15:
                    return "> 24 hr & < 15 tests"
                else:
                    return "valid"
            else:
                return "< 30 tests"
        else:
            return "valid"
                
    #Apply above function.
    summary["validity"] = summary.apply(
            lambda row: check_valid_sites(row), axis=1)

    invalid = summary[summary["validity"] != "valid"]

    tests["clean_loc"] = tests.apply(
            lambda row: int(row.site.split("-")[0]), axis=1)
    for site in invalid["site"].unique():
        tests = tests[tests["clean_loc"] != site]

    print(summary)
    return tests, summary


def filter_and_count(data, param="", value="", count=""):
    """ Filter a column based on value, and/or count occurencies of the 
    different values in a given column."""

    # Value filter part.
    if param:
        res = data[data[param] == value]

    # Value occurency count part. 
    if count:
        res = data.groupby(count).size().reset_index(name="count")

    return res

def eval_tests(data):
    """ Check if test is passed or failed on dowload, upload and both
    directions. Add columns with this information to data."""

    def eval_func(row, direction):
        expected = getattr(row, "exp_"+direction+"_br")
        found = getattr(row, direction+"_br")
        if expected > found:
            return False 
        return True

    # Use eval_func to check tests.
    data["dn_pass"] = data.apply(
            lambda row: eval_func(row, "dn"), axis=1)
    data["up_pass"] = data.apply(
            lambda row: eval_func(row, "up"), axis=1)

    data["pass"] = data.dn_pass & data.up_pass

    return data

def check_compliance(data):
    """ Evaluate the rules to check service compliance according to 
    contract. Test data must belong to only one profile, else function 
    will perform wrongly."""

    profile_compliance = pd.DataFrame()

    # Save expected performance.
    exp_dn_br = data["exp_dn_br"].unique()[0]
    exp_up_br = data["exp_up_br"].unique()[0]

    for hour in range(6,21):
     
        # Filter based on test timestamp. Do not use "hour" column. It 
        # has been found to have invalid info. 
        hour_data = data[data["timestamp"].dt.hour == hour]
        if hour_data.empty:
            continue

        # This is the 5 % of X for the tests in a given hour.
        dn_nth_value = hour_data["dn_br"].quantile(
                0.05, interpolation="nearest")
        up_nth_value = hour_data["up_br"].quantile(
                0.05, interpolation="nearest")

        # Evaluate if 5 % of X is above expected bit rate. 
        dn_pass = True if dn_nth_value >= exp_dn_br else False
        up_pass = True if up_nth_value >= exp_up_br else False

        # Check if tests pass.
        eval_data = eval_tests(hour_data)

        # Count passed and failed tests.
        dn_pass_count = len(eval_data[eval_data["dn_pass"] == True])
        up_pass_count = len(eval_data[eval_data["up_pass"] == True])

        count = len(hour_data)

        hour_compliance = pd.DataFrame({
                "hour": hour,
                "count": count,
                "exp_dn_br": exp_dn_br,
                "exp_up_br": exp_up_br,
                "dn_nth_value": dn_nth_value,
                "up_nth_value": up_nth_value,
                "avg_dn_br": np.average(hour_data["dn_br"]),
                "avg_up_br": np.average(hour_data["up_br"]),
                "dn_pass": dn_pass,
                "up_pass": up_pass,
                "pass": dn_pass & up_pass,
                "dn_pass_count": dn_pass_count,
                "up_pass_count": up_pass_count,
                "dn_pass_%": dn_pass_count*100/count,
                "up_pass_%": up_pass_count*100/count} ,index=[0])

        # Append hour data to profile data.
        profile_compliance = pd.concat(
                [profile_compliance, hour_compliance],
                ignore_index = True, axis=0)

    return profile_compliance

def profile_summary(data, profile):
    """ Calculate penalty and summarize profile behavior based on the 
    profile compliance data."""
    
    # Count how many hours passed.
    passed_hours = len(data[data["pass"] == True])
    failed_hours = len(data[data["pass"] == False])

    summary = pd.DataFrame({
            "profile": profile,
            "passed_hours": passed_hours,
            "failed_hours": failed_hours}, index=[0])

    return summary

def get_compliance_report(tests_data, tickets_data):
    print("################### asi entra a compliance report")
    print(tests_data)
    valid_tests, site_validity = filter_test_qty_tkt(
            tests_data, tickets_data, 30, 15)

    print("################# esto es despues de filtrar los tickets")
    print(valid_tests)
    summary = pd.DataFrame()

    timestamp = datetime.strftime(datetime.now(), "%d%m%y_%H%M%S")
    filename = secure_filename("Compliance_" + timestamp + ".xlsx")
    path =  os.path.join(current_app.config["REPORT_FOLDER"], filename)

    with pd.ExcelWriter(path) as writer:

        site_validity.to_excel(writer, sheet_name="Valid Sites")
        for profile in valid_tests["profile"].unique():

            # Build sheet name prefix with download and upload speed 
            # information. Eg: 12X3

            sheet_name_prefix = ""
            sheet_name_prefix += profile.split("-")[0].split(":")[1]
            sheet_name_prefix += "X"
            sheet_name_prefix += profile.split("-")[1].split(":")[1]

            # Get profile tests.
            profile_data = filter_and_count(
                    valid_tests, param="profile",value=profile)
            print("################ este es el perfil")
            print(profile)
            print("estos son los tests")
            print(profile_data)

            # Check compliance with all sites.
            compliance_data = check_compliance(profile_data)
            print("########## esto es el compliance data de ese perfil")
            print(compliance_data)
            sheet_name = sheet_name_prefix + " Compliance"
            compliance_data.to_excel(writer, sheet_name=sheet_name)

            # Calculate summary all sites.
            profile_summary_data = profile_summary(
                    compliance_data, profile)
            summary = pd.concat([summary, profile_summary_data],
                    ignore_index = True, axis=0)
            print("############# esto es el summary")
            print(summary)

        summary.to_excel(writer, sheet_name="Payment Summary")
        print("################### este es el path")
        print(path)
        return path
