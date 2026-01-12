import logging
from functools import partial
import pandas as pd

from . import bro

logger = logging.getLogger(__name__)


class SoilFaceResearch(bro.FileOrUrl):
    """Class to represent a Soil Face Research (SFR) from the BRO.

    The configuration of the xml-file can be found at
    https://www.bro-productomgeving.nl/__attachments/1607470159/DO_ResponseSFR_O_DP.xml?inst-v=a371ba4a-5fb1-479c-99cb-44e36b9c21f9
    (link found at https://www.bro-productomgeving.nl/bpo/latest/uitgifte-voorbeeldberichten-sfr)
    """

    _rest_url = "https://publiek.broservices.nl/sr/sfr/v2"
    _xmlns = "http://www.broservices.nl/xsd/dssfr/2.0"
    _char = "SFR_C"

    def _read_contents(self, tree):
        ns = {
            "brocom": "http://www.broservices.nl/xsd/brocommon/3.0",
            "gml": "http://www.opengis.net/gml/3.2",
            "sfrcom": "http://www.broservices.nl/xsd/sfrcommon/2.0",
            "xmlns": self._xmlns,
        }
        sfrs = tree.findall(".//xmlns:SFR_O", ns)
        if len(sfrs) != 1:
            raise (Exception("Only one SFR_O supported"))
        sfr = sfrs[0]
        for key in sfr.attrib:
            setattr(self, key.split("}", 1)[1], sfr.attrib[key])
        for child in sfr:
            key = self._get_tag(child)
            if len(child) == 0:
                setattr(self, key, child.text)
            elif key == "deliveredLocation":
                self._read_delivered_location(child)
            elif key == "deliveredVerticalPosition":
                to_float = ["offset"]
                self._read_children_of_children(child, to_float=to_float)
            elif key == "standardizedLocation":
                self._read_standardized_location(child)
            elif key in ["researchReportDate", "fieldworkDate"]:
                setattr(self, key, self._read_date(child))
            elif key in ["registrationHistory", "reportHistory"]:
                self._read_children_of_children(child)
            elif key in ["researchOperator"]:
                setattr(self, key, self._read_operator(child))
            elif key == "siteCharacteristic":
                if self._check_single_child_with_tag(child, "SiteCharacteristic"):
                    child = child[0]
                self._read_children_of_children(child)
            elif key == "soilUncovering":
                if self._check_single_child_with_tag(child, "SoilUncovering"):
                    child = child[0]
                self._read_children_of_children(child)
            elif key == "soilFaceDescription":
                if self._check_single_child_with_tag(child, "SoilFaceDescription"):
                    child = child[0]
                self._read_soil_face_description(child)
            elif key == "soilFaceSampleAnalysis":
                if self._check_single_child_with_tag(child, "SoilFaceSampleAnalysis"):
                    child = child[0]
                self._read_soil_face_sample_analysis(child)
            else:
                self._warn_unknown_tag(key)

        if hasattr(self, "litterLayer"):
            self.litterLayer = pd.DataFrame(self.litterLayer)
        if hasattr(self, "soilLayer"):
            self.soilLayer = pd.DataFrame(self.soilLayer)
        if hasattr(self, "disturbedInterval"):
            self.disturbedInterval = pd.DataFrame(self.disturbedInterval)
        if hasattr(self, "investigatedInterval"):
            self.investigatedInterval = pd.DataFrame(self.investigatedInterval)

    def _read_soil_face_sample_analysis(self, node):
        for child in node:
            key = self._get_tag(child)
            if key == "analysisReportDate":
                setattr(self, key, self._read_date(child))
            elif key in ["analysisType"]:
                setattr(self, key, child.text)
            elif key in ["analysisOperator"]:
                setattr(self, key, self._read_operator(child))
            elif key == "investigatedInterval":
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key == "InvestigatedInterval":
                        d = self._read_investigated_interval(grandchild)
                        if hasattr(self, "investigatedInterval"):
                            self.investigatedInterval.append(d)
                        else:
                            self.investigatedInterval = [d]
                    else:
                        self._warn_unknown_tag(key)
            elif key in []:
                setattr(self, key, float(child.text))
            else:
                self._warn_unknown_tag(key)

    def _read_investigated_interval(self, node):
        d = {}
        for child in node:
            key = self._get_tag(child)
            if key in ["beginDepth", "endDepth"]:
                d[key] = float(child.text)
            elif key in ["characteristicModelled"]:
                d[key] = child.text
            elif key == "pHDetermination":
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key == "PHDetermination":
                        self._read_ph_determination(grandchild, d)
                    else:
                        self._warn_unknown_tag(key)
            elif key == "particleSizeDistributionDetermination":
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key == "ParticleSizeDistributionDetermination":
                        self._read_particle_size_distribution_determination(
                            grandchild, d
                        )
                    else:
                        self._warn_unknown_tag(key)
            elif key == "shrinkageDetermination":
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key == "ShrinkageDetermination":
                        self._read_shrinkage_determination(grandchild, d=d)
                    else:
                        self._warn_unknown_tag(key)
            else:
                self._warn_unknown_tag(key)
        return d

    def _read_shrinkage_determination(self, node, d):
        for child in node:
            key = self._get_tag(child)
            if key in [
                "determinationProcedure",
                "determinationMethod",
                "disturbed",
            ]:
                d[key] = child.text
            elif key == "shrinkage":
                for child in node:
                    key = self._get_tag(child)
                    if key == "DataArray":
                        d["shrinkage"] = self._read_data_array(child)
                    else:
                        self._warn_unknown_tag(key)
            else:
                self._warn_unknown_tag(key)

    def _read_particle_size_distribution_determination(self, node, d):
        for child in node:
            key = self._get_tag(child)
            if key in [
                "determinationProcedure",
                "determinationMethod",
                "particleSizeDistributionStandardised",
                "fractionDistribution",
                "performanceIrregularity",
                "dispersionMethod",
            ]:
                d[key] = child.text
            elif key == "nonStandardisedFraction":
                d2 = {}
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key in [
                        "lowerBoundary",
                        "upperBoundary",
                        "proportion",
                    ]:
                        d2[key] = float(grandchild.text)
                    else:
                        self._warn_unknown_tag(key)
                    if "nonStandardisedFraction" in d:
                        d["nonStandardisedFraction"].append(d2)
                    else:
                        d["nonStandardisedFraction"] = [d2]
            else:
                self._warn_unknown_tag(key)
        if "nonStandardisedFraction" in d:
            d["nonStandardisedFraction"] = pd.DataFrame(d["nonStandardisedFraction"])

    def _read_ph_determination(self, node, d):
        for child in node:
            key = self._get_tag(child)
            if key in ["determinationProcedure", "determinationMethod"]:
                # capitalize key but keep rest as is, and prepend pH
                key = f"pH{key[0].upper()}{key[1:]}"
                d[key] = child.text
            elif key == "pH":
                d[key] = float(child.text)
            else:
                self._warn_unknown_tag(key)

    def _read_soil_face_description(self, node):
        for child in node:
            key = self._get_tag(child)
            if key == "descriptionReportDate":
                setattr(self, key, self._read_date(child))
            elif key in [
                "descriptionProcedure",
                "describedWidth",
                "artificiallyHumidified",
                "fractionDistributionDetermined",
                "lowerBoundarySandFraction",
            ]:
                setattr(self, key, child.text)
            elif key == "descriptionOperator":
                setattr(self, key, self._read_operator(child))
            elif key == "soilProfile":
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key == "SoilProfile":
                        self._read_soil_profile(grandchild)
                    else:
                        self._warn_unknown_tag(key)
            elif key == "soilClassification":
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key == "SoilClassification":
                        setattr(self, key, self._read_soil_classification(grandchild))
                    else:
                        self._warn_unknown_tag(key)
            else:
                self._warn_unknown_tag(key)

    def _read_soil_classification(self, node):
        d = {}
        for child in node:
            key = self._get_tag(child)
            if key in [
                "codeGroup",
                "classificationCode",
                "specialFeatureTop",
                "soilClass",
                "textureClass",
                "peatClass",
                "subsoilPeat",
                "lowerBoundaryPeat",
                "subsoilDuinVagueSoil",
                "textureProfile",
                "carbonateProfile",
                "reworkingClass",
                "groundwaterTableClass",
                "anomalousGroundwaterRegime",
                "specialFeatureSite",
            ]:
                d[key] = child.text
            elif key == "specialFeatureBottom":
                sfb = {}
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key == "specialFeature":
                        sfb[key] = grandchild.text
                    elif key == "beginDepth":
                        sfb[key] = float(grandchild.text)
                    else:
                        self._warn_unknown_tag(key)
                if "specialFeatureBottom" in d:
                    d["specialFeatureBottom"].append(sfb)
                else:
                    d["specialFeatureBottom"] = [sfb]
            else:
                self._warn_unknown_tag(key)
        return d

    def _read_soil_profile(self, node):
        for child in node:
            key = self._get_tag(child)
            if key in [
                "descriptionQuality",
                "rootableDepthReached",
                "meanHighestGroundwaterLevelReached",
                "horizonRepetition",
                "upperBoundaryShape",
                "sequenceDisturbed",
                "compactionPresent",
            ]:
                setattr(self, key, child.text)
            elif key in [
                "rootableDepth",
                "meanHighestGroundwaterLevel",
                "meanLowestGroundwaterLevel",
            ]:
                setattr(self, key, float(child.text))
            elif key in ["localPhenomenon"]:
                if hasattr(self, key):
                    getattr(self, key).append(child.text)
                else:
                    setattr(self, key, [child.text])
            elif key == "litterLayer":
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key == "LitterLayer":
                        self._read_litter_layer(grandchild)
                    else:
                        self._warn_unknown_tag(key)
            elif key == "soilLayer":
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key == "SoilLayer":
                        self._read_soil_layer(grandchild)
                    else:
                        self._warn_unknown_tag(key)
            elif key == "disturbedInterval":
                d = {}
                self._read_children_of_children(child, d=d)
                if hasattr(self, "disturbedInterval"):
                    self.disturbedInterval.append(d)
                else:
                    self.disturbedInterval = [d]
            elif key == "compactedInterval":
                d = {}
                self._read_children_of_children(child, d=d)
                self.compactedInterval = d
            else:
                self._warn_unknown_tag(key)

    def _read_litter_layer(self, node):
        layer = {}
        for child in node:
            key = self._get_tag(child)
            if key in [
                "upperBoundary",
                "lowerBoundary",
            ]:
                layer[key] = float(child.text)
            elif key in [
                "upperBoundaryDetermination",
                "lowerBoundaryDetermination",
                "lowerBoundaryShape",
                "layerDiscontinuous",
                "horizonCode",
                "litterType",
                "estimatedOrganicMatterContent",
            ]:
                layer[key] = child.text
            else:
                self._warn_unknown_tag(key)
        if hasattr(self, "litterLayer"):
            self.litterLayer.append(layer)
        else:
            self.litterLayer = [layer]

    def _read_soil_layer(self, node):
        layer = {}
        for child in node:
            key = self._get_tag(child)
            if key in [
                "upperBoundary",
                "lowerBoundary",
                "particleDensity",
                "fieldCapacity",
                "wiltingPoint",
                "saturationWaterContent",
                "hydraulicConductivity",
                "bulkDensity",
                "stoneContent",
                "clayFraction",
                "siltFraction",
                "sandFraction",
            ]:
                layer[key] = float(child.text)
            elif key in [
                "upperBoundaryDetermination",
                "lowerBoundaryDetermination",
                "lowerBoundaryShape",
                "layerDiscontinuous",
                "anthropogenic",
                "mixed",
                "inverted",
                "rooted",
                "rootsEvenlyDistributed",
                "rootAbundanceClass",
                "slant",
            ]:
                layer[key] = child.text
            elif key == "soilLife":
                if key in layer:
                    layer[key].append(child.text)
                else:
                    layer[key] = [child.text]
            elif key == "homogeneousMaterial":
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key in [
                        "specialMaterial",
                        "horizonCode",
                        "rockType",
                        "depositionalCharacteristic",
                    ]:
                        layer[key] = grandchild.text
                    elif key in ["estimatedSaturatedPermeability"]:
                        layer[key] = float(grandchild.text)
                    elif key == "soil":
                        self._read_soil(grandchild, layer)
                    elif key == "rock":
                        self._read_rock(grandchild, layer)
                    else:
                        self._warn_unknown_tag(key)
            elif key == "layerComponent":
                self._read_layer_component(node, layer, layer=layer)
            else:
                self._warn_unknown_tag(key)

        if "layerComponent" in layer:
            layer["layerComponent"] = pd.DataFrame(layer["layerComponent"])
        if hasattr(self, "soilLayer"):
            self.soilLayer.append(layer)
        else:
            self.soilLayer = [layer]

    def _read_layer_component(self, node, d=None):
        component = {}
        self._read_children_of_children(node, d=component)
        if "layerComponents" not in d:
            d["layerComponents"] = []
        d["layerComponents"].append(component)


cl = SoilFaceResearch

get_bro_ids_of_bronhouder = partial(bro._get_bro_ids_of_bronhouder, cl)
get_bro_ids_of_bronhouder.__doc__ = bro._get_bro_ids_of_bronhouder.__doc__

get_data_for_bro_ids = partial(bro._get_data_for_bro_ids, cl)
get_data_for_bro_ids.__doc__ = bro._get_data_for_bro_ids.__doc__

get_characteristics = partial(bro._get_characteristics, cl)
get_characteristics.__doc__ = bro._get_characteristics.__doc__

get_data_in_extent = partial(bro._get_data_in_extent, cl)
get_data_in_extent.__doc__ = bro._get_data_in_extent.__doc__
