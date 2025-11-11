import pandas as pd
import os
import warnings

# silencing an annoying warning because I'm petty
warnings.filterwarnings("ignore", message="DataFrameGroupBy.apply operated on the grouping columns")

# =====================================================================================
# CONFIG
# =====================================================================================

# main
ROOT = "../data/canvas-data/Global Health +AI course fall 2025"
IMAGES_DIR = os.path.join(ROOT, "images_cropped_padded")
LABELS = os.path.join(ROOT, "all_labels.csv")
OUTPUT_DIR = os.path.join("../pipeline/selected_images.xlsx")


# rare
RARE_IMAGES_DIR = os.path.join(ROOT, "rare anopheles")

# colombia
COLOMBIA = os.path.join(RARE_IMAGES_DIR, "Colombia")
CIMAGES1 = os.path.join(COLOMBIA, "Anopheles_Albimanus")
CIMAGES2 = os.path.join(COLOMBIA, "Anopheles_Darlingi_Anopheles_Nuneztovari")

CLABELS1 = os.path.join(COLOMBIA, "An_Albimanus.csv")
CLABELS2 = os.path.join(COLOMBIA, "An_Darlingi_An_Nuneztovari.csv")

# mozambique
MOZAMBIQUE = os.path.join(RARE_IMAGES_DIR, "Mozambique")
MIMAGES = os.path.join(MOZAMBIQUE, "Mozambique_images_crop_pad")
MLABELS = os.path.join(MOZAMBIQUE, "All_mozambique_specimens_AC_PH_AG.csv")

# uganda
UGANDA = os.path.join(RARE_IMAGES_DIR, "Uganda")
UIMAGES = os.path.join(UGANDA, "Coustani_images")
ULABELS = os.path.join(UGANDA, "all_uganda_Jan_Feb_coustani.csv")

TOTAL_IMAGES = 400
SEED = 42


# =====================================================================================
# HELPERS
# =====================================================================================

def add_image_path(df, folder, source):
    """Add full image path + source column."""
    df["image_path"] = df["Image_name"].apply(lambda x: os.path.join(folder, x))
    df["source"] = source
    return df


# =====================================================================================
# LOAD DATASETS
# =====================================================================================

# main (non-rare)
df_main = pd.read_csv(LABELS)
df_main = add_image_path(df_main, IMAGES_DIR, "non-rare")

# colombia
df_col1 = pd.read_csv(CLABELS1)
df_col1 = add_image_path(df_col1, CIMAGES1, "colombia_albimanus")
df_col2 = pd.read_csv(CLABELS2)
df_col2 = add_image_path(df_col2, CIMAGES2, "colombia_darlingi_nuneztovari")

# Mozambique
df_moz = pd.read_csv(MLABELS)
df_moz = add_image_path(df_moz, MIMAGES, "mozambique")

# Uganda
df_uga = pd.read_csv(ULABELS)
df_uga = add_image_path(df_uga, UIMAGES, "uganda")



# =====================================================================================
# COMBINE + CLEAN
# =====================================================================================

# dichotomous key mosquitos
keys = {
    0: "funestus",
    1: "gambiae",
    10: "coustani",       # apparently many species belong to this complex group so we arbitrarly label this as 10
    9: "pharoensis",
    8: "tenebrosus"
}

keep = [0, 1, 8, 9, 10]

# drop all mosquitos that are not labelled species = 0 or 1
# 0 = Anopheles funestus
# 1 = Anopheles gambiae
# df_main = df_main[df_main["species"].isin(keep)]


# we arbitrarily label this as 10 to indicate a different species
df_uga["species"] = 10


# combine all datasets and drop any duplicates
combined = pd.concat([df_main, df_col1, df_col2, df_moz, df_uga], ignore_index=True)
df = combined.drop_duplicates(subset=["Image_name", "image_path"], keep="first")
df = df[df["species"].isin(keep)]


# drop all unnecessary columns and add on the species names
cols = ["Image_name", "species", "image_path"]
df = df[cols]


# create a new column called species name to map species index -> species name
df["species_name"] = df["species"].map(keys)


# =====================================================================================
# SAMPLE 400 IMAGES
# =====================================================================================

# figure out how many species are represented here and figure out how many you need
# per species to get roughly an even number
species_counts = df["species"].nunique()
per_species = max(1, TOTAL_IMAGES // species_counts)

df = df.reset_index(drop=True)
sampled = (
    df.groupby("species", group_keys=False)
      .apply(lambda x: x.sample(
          n=min(per_species, len(x)),
          random_state=SEED
      ))
      .reset_index(drop=True)
)



if len(sampled) < TOTAL_IMAGES:
    remaining = TOTAL_IMAGES - len(sampled)
    extra = df.drop(sampled.index).sample(n=remaining, random_state=SEED)
    sampled = pd.concat([sampled, extra], ignore_index=True)
    
    
# =====================================================================================
# MAP TO COMPACT DESCRIPTIONS
# =====================================================================================

# Config
ROOT_DIR = "../../dichotmous-keys"
COMPACT_DIR = os.path.join(ROOT_DIR, "compact_descriptions.csv")

# load descriptions
compact_df = pd.read_csv(COMPACT_DIR, encoding="latin1")

# create a mapping
compact_map = {}
compact_map[0] = compact_df.loc[compact_df["Species"] == keys[0], "Full description"].iloc[0]
compact_map[1] = compact_df.loc[compact_df["Species"] == keys[1], "Full description"].iloc[0]
compact_map[8] = compact_df.loc[compact_df["Species"] == keys[8], "Full description"].iloc[0]
compact_map[9] = compact_df.loc[compact_df["Species"] == keys[9], "Full description"].iloc[0]
compact_map[10] = compact_df.loc[compact_df["Species"] == keys[10], "Full description"].iloc[0]


# quick sanity check that all the species we want are in here
# dat = compact_df
# print("coustani" in dat["Species"].values)
# print("pharoensis" in dat["Species"].values) # mozambique
# print("tenebrosus" in dat["Species"].values) # mozambique
# print("gambiae" in dat["Species"].values)
# print("funestus" in dat["Species"].values)


# map the descriptions
sampled["compact_descriptions"] = sampled["species"].map(compact_map)

    
sampled.to_excel(OUTPUT_DIR, index=False)
print(f"Saved {len(sampled)} sampled images to {OUTPUT_DIR}")