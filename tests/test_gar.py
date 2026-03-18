import os

import brodata


def test_gar_report_from_xml_file_multiple_analysis_processes():
    fname = os.path.join("tests", "data", "GAR000000019636.xml")
    brodata.gar.GroundwaterAnalysisReport(fname)


def test_gar_report_from_xml_file_single_analysis_process():
    fname = os.path.join("tests", "data", "GAR000000042563.xml")
    brodata.gar.GroundwaterAnalysisReport(fname)


def test_gar_report_from_csv_file():
    fname = os.path.join("tests", "data", "GAR000000042563.csv")
    brodata.gar.GroundwaterAnalysisReport(fname)


def test_gar_report():
    brodata.gar.GroundwaterAnalysisReport.from_bro_id("GAR000000019636")


def test_gar_get_parameter_list():
    brodata.gar.get_parameter_list()
