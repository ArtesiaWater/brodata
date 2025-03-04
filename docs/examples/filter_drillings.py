# %%
import brodata
from tqdm.auto import tqdm

# %% Download metadata
extent = [118000, 118400, 439560, 440100]

gdf_meta = brodata.dino.get_gdf(kind="Geologisch booronderzoek", extent=extent)

# %% Filter metadata


# %% download individual drillings
data = {}
for dino_nr in tqdm(gdf_meta.index):
    data[dino_nr] = brodata.dino.GeologischBooronderzoek.from_dino_nr(dino_nr)

# %% transform individual drillings to gdf
gdf = brodata.dino.objects_to_gdf(data)
