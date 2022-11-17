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
        "Error": "error"}

import pandas as pd

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
        df = self.eval_tests()["pass"].value_counts()
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
        df = self.eval_tests()

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

        df = self.eval_tests()

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

    reporter = Reporter(clean)
    return reporter 
