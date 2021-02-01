#include <hdf5.h>
extern herr_t H5Pset_vol_async(hid_t fapl_id);
extern herr_t H5Fwait(hid_t file_id, hid_t dxpl_id);
extern herr_t H5Dwait(hid_t dset_id, hid_t dxpl_id);
