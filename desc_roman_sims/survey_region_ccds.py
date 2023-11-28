import os
from collections import namedtuple
from functools import wraps
import warnings
import sqlite3
from erfa import ErfaWarning
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.path import Path
from matplotlib import patches
from astropy.time import Time
import pandas as pd
import galsim
from lsst.afw import cameraGeom
import lsst.geom
from imsim import load_telescope, BatoidWCSBuilder, get_camera


__all__ = ['ignore_erfa_warnings', 'SurveyRegion', 'CcdRegionFactory',
           'lonlat']


def ignore_erfa_warnings(func):
    @wraps(func)
    def call_func(*args, **kwargs):
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', 'ERFA', ErfaWarning)
            return func(*args, **kwargs)
    return call_func


def lonlat(unit_vector):
    return (lsst.sphgeom.LonLat.longitudeOf(unit_vector).asDegrees(),
            lsst.sphgeom.LonLat.latitudeOf(unit_vector).asDegrees())


def make_patch(sky_polygon):
    vertices = []
    for vertex in sky_polygon.getVertices():
        vertices.append(
            (lsst.sphgeom.LonLat.longitudeOf(vertex).asDegrees(),
             lsst.sphgeom.LonLat.latitudeOf(vertex).asDegrees())
        )
    vertices.append((0, 0))
    codes = [Path.MOVETO,
             Path.LINETO,
             Path.LINETO,
             Path.LINETO,
             Path.CLOSEPOLY,
             ]
    return Path(vertices, codes)


class SurveyRegion:
    """
    Class to define a survey region and provide functions to find
    overlapping convex polygons.
    """
    def __init__(self, ra0, dec0, lon_size, lat_size):
        self.cos_dec = np.cos(np.radians(dec0))
        self.ra_min = ra0 - lon_size/2./self.cos_dec
        self.ra_max = ra0 + lon_size/2./self.cos_dec
        self.dec_min = dec0 - lat_size/2.
        self.dec_max = dec0 + lat_size/2.
        self._make_polygon()
        self.center = galsim.CelestialCoord(ra0*galsim.degrees,
                                            dec0*galsim.degrees)
        self.size = min(lon_size, lat_size)

    def _make_polygon(self):
        corners = [(self.ra_min, self.dec_min),
                   (self.ra_max, self.dec_min),
                   (self.ra_max, self.dec_max),
                   (self.ra_min, self.dec_max)]
        vertices = [lsst.sphgeom.UnitVector3d(
            lsst.sphgeom.LonLat.fromDegrees(*corner)) for corner in corners]
        self.polygon = lsst.sphgeom.ConvexPolygon(vertices)

    def intersects(self, polygon):
        return self.polygon.intersects(polygon)

    def draw_boundary(self, color=None):
        ra = (self.ra_min, self.ra_max, self.ra_max, self.ra_min, self.ra_min)
        dec = (self.dec_min, self.dec_min, self.dec_max, self.dec_max,
               self.dec_min)
        plt.plot(ra, dec, color=color)


class CcdRegionFactory:
    """Class to create ConvexPolygon objects covering CCD sky regions
    for a specified observation pointing.
    """
    @ignore_erfa_warnings
    def __init__(self, mjd, ra, dec, band, rottelpos, camera_name="LsstCam",
                 fov_radius=None):
        """
        Parameters
        ----------
        mjd : float
            Mean Julian Date of the observation.
        ra : float
            RA of the pointing in degrees.
        dec : float
            Dec of the pointing in degrees.
        band : str
            Band of observation, e.g., 'u', 'g', 'r', 'i', 'z', 'y'.
        rottelpos : float
            Angle of telescope rotator wrt the mount in degrees.
        camera_name : str ['LsstCam']
            Camera class name.
        fov_radius : float [None]
            Radius of field-of-view, enclosing all CCDS, in degrees.
            If None, the use LSSTCam FOV radius of 1.76 degrees
            (Ref. LCA-13381).
        """
        obstime = Time(mjd, format='mjd')
        self.boresight \
            = galsim.CelestialCoord(ra*galsim.degrees, dec*galsim.degrees)
        telescope = load_telescope(f"LSST_{band}.yaml",
                                   rotTelPos=rottelpos*galsim.degrees)
        self.factory = BatoidWCSBuilder().makeWCSFactory(
            self.boresight, obstime, telescope, bandpass=band,
            camera=camera_name)

        self.camera = get_camera(camera_name)
        self.fov_radius = fov_radius if fov_radius is not None else 1.76

    @ignore_erfa_warnings
    def create(self, det):
        """Return a ConvexPolygon corresponding to the sky region for the
        specified Detector object.

        Parameters
        ----------
        det : lsst.afw.cameraGeom.Detector
            Detector object.

        Returns
        -------
        lsst.sphgeom.ConvexPolygon
        """
        if isinstance(det, str):
            det = self.camera[det]
        wcs = self.factory.getWCS(det)
        vertices = []
        for corner in det.getCorners(cameraGeom.PIXELS):
            sky_coord = wcs.toWorld(galsim.PositionD(corner.x, corner.y))
            lonlat = lsst.sphgeom.LonLat.fromDegrees(sky_coord.ra.deg,
                                                     sky_coord.dec.deg)
            vertices.append(lsst.sphgeom.UnitVector3d(lonlat))
        return lsst.sphgeom.ConvexPolygon(vertices)

    def draw_focal_plane(self, ax, ccds=None, region=None, color=None):
        """
        Draw the selected CCDs on the specified matplotlib axes.
        """
        if ccds is None:
            ccds = self.select_ccds(region)
        for det_name in ccds:
            polygon = self.create(det_name)
            self.draw_sky_polygon(ax, polygon, color=color)

    def select_ccds(self, region=None):
        """
        Return the set of science CCDs within the specified region.
        If region is None, return all science CCDs.
        """
        select_all = region is None
        if not select_all:
            # Check the offset of the broresight to the region center
            # to test if the FOV is entirely enclosed in the
            # region. If so, select all of the science CCDs.
            offset = region.center.distanceTo(self.boresight)/galsim.degrees
            if offset < (region.size/2. - self.fov_radius):
                select_all = True

        ccds = set()
        for det in self.camera:
            if det.getType() != cameraGeom.DetectorType.SCIENCE:
                # Skip non-science CCDs
                continue
            if select_all or region.intersects(self.create(det)):
                ccds.add(det.getName())
        return ccds

    @staticmethod
    def draw_sky_polygon(ax, polygon, alpha=0.2, lw=1, color=None):
        """
        Draw the patch corresponding to the convex polygon.
        """
        path = make_patch(polygon)
        ax.add_patch(patches.PathPatch(path, alpha=alpha, lw=lw, color=color))


ObsInfo = namedtuple('ObsInfo', ['mjd', 'ra', 'dec', 'band', 'rottelpos'])


class OpSimData:
    def __init__(self, opsim_db_file, query_conditions=None):
        query = "select * from observations"
        if query_conditions is not None:
            query += f" {query_conditions}"
        assert os.path.isfile(opsim_db_file)
        with sqlite3.connect(opsim_db_file) as con:
            self.df = pd.read_sql(query, con)

    def obs_info(self, visit):
        row = self.df.query(f"observationId == {visit}").iloc[0]
        return ObsInfo(row['observationStartMJD'], row['fieldRA'],
                       row['fieldDec'], row['filter'], row['rotTelPos'])

    def ccd_region_factory(self, visit, fov_radius=None):
        return CcdRegionFactory(*self.obs_info(visit), fov_radius=fov_radius)

    def __len__(self):
        return len(self.df)


if __name__ == '__main__':
    ra0, dec0 = 55, -39
    mjd = 60800.
    band = 'i'
    rottelpos = 0.
    camera_name = 'LsstCam'

    polygon_factory = CcdRegionFactory(mjd, ra0, dec0, band, rottelpos,
                                       camera_name)

    det_name = 'R22_S11'
    polygon = polygon_factory.create(det_name)
