import seaborn as sns
from matplotlib import pyplot as plt

HIGHERISBETTER = "Higher is better ↑"
LOWERISBETTER = "Lower is better ↓"
FONT_SIZE = 12
ISBETTER_FONTSIZE = FONT_SIZE + 2

WIDE_FIGSIZE = (13, 2.8)
ULTRA_WIDE_FIGSIZE = (16, 2.8)
COLUMN_FIGSIZE = (6.5, 3.4)

FIGURE_SIZE = (7, 4)
COLUMN_FIG_SIZE = (6.5, 3.4)
COLORS = sns.color_palette("pastel")
HATCHES = [
    "/",
    "|",
    "-",
    "x",
    ".",
    ",",
    "*",
    "o",
    "O",
    "+",
    "X",
    "s",
    "S",
    "d",
    "D",
    "^",
    "v",
    "<",
    ">",
    "p",
    "P",
    "$",
    "#",
    "%",
]

sns.set_theme(style="whitegrid", color_codes=True)
plt.rcParams.update({"font.size": FONT_SIZE})
