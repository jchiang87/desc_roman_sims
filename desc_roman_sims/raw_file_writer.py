import os
import logging
import galsim
import imsim

all = ["RawFileWriter"]


class RawFileWriter:
    """Class to write raw files from eimage files"""
    def __init__(self, readout_time=3., dark_current=0.02, camera=None,
                 pcti=1e-6, scti=1e-6, bias_level=1000., bias_levels_file=None,
                 logger=None, rng=None):
        self.readout_params = dict(readout_time=readout_time,
                                   dark_current=dark_current,
                                   camera=camera, pcti=pcti, scti=scti,
                                   bias_level=bias_level,
                                   bias_levels_file=bias_levels_file)
        if logger is None:
            self.logger = logging.getLogger()
        else:
            self.logger = logger
        if rng is None:
            self.rng = galsim.BaseDeviate()
        else:
            self.rng = rng

    @staticmethod
    def _read_eimage(eimage_file):
        eimage = galsim.fits.read(eimage_file)
        hdus = galsim.fits.readFile(eimage_file)
        eimage.header = hdus[0].header
        wcs, _ = galsim.fits.readFromFitsHeader(eimage.header)
        eimage.wcs = wcs
        return eimage

    def write(self, eimage_file, outfile=None):
        if outfile is None:
            outfile = (os.path.basename(eimage_file).replace("eimage", "amp")
                       + ".fz")
        eimage = self._read_eimage(eimage_file)

        ccd_readout = imsim.CcdReadout(eimage, self.logger,
                                       **self.readout_params)
        hdus = ccd_readout.prepare_hdus(self.rng)
        ccd_readout.write_raw_file(hdus, outfile)
