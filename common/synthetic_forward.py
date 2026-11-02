from typing import Optional

import numpy as np
import pandas as pd

NEAREST_STRIKES = 5
MIN_VALID_STRIKES = 3


def synthetic_forward(chain: pd.DataFrame, spot: float) -> Optional[float]:
    """Put-call-parity forward (K + C - P) for the option chain's own expiry: median over the
    strikes nearest spot with a positive price on both sides. None if too few qualify."""
    # Quotes are LTP-only (no depth), so the median is what guards against one stale print.
    calls = chain[chain["instrument_type"] == "CE"].set_index("strike")["price"]
    puts = chain[chain["instrument_type"] == "PE"].set_index("strike")["price"]
    strikes = [k for k in calls.index.intersection(puts.index) if calls[k] > 0 and puts[k] > 0]
    nearest = sorted(strikes, key=lambda k: abs(k - spot))[:NEAREST_STRIKES]
    if len(nearest) < MIN_VALID_STRIKES:
        return None
    return float(np.median([k + calls[k] - puts[k] for k in nearest]))
