import logging

from scipy.stats import wasserstein_distance

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def extract_percentiles(stats: dict) -> list:
    """
    Extracts the 25th, 50th, and 75th percentiles from a statistics dictionary.
    
    If a percentile key ("p25", "p50", or "p75") is missing, its value defaults to 0.
    
    Returns:
        A list containing the values for p25, p50, and p75 percentiles.
    """
    p25 = stats.get("p25", 0)
    p50 = stats.get("p50", 0)
    p75 = stats.get("p75", 0)
    return [p25, p50, p75]


def extract_statistics(stats: dict) -> list:
    """
    Extracts a comprehensive set of statistics from a dictionary, defaulting missing values to 0.
    
    The returned list includes min, max, mean, median, standard deviation, unique count, null count, and the 25th, 50th, and 75th percentiles (from a nested "percentiles" dictionary if present).
    
    Args:
        stats: Dictionary containing statistical metrics.
    
    Returns:
        List of numeric values representing the extracted statistics in a fixed order.
    """
    min_val = stats.get("min", 0)
    max_val = stats.get("max", 0)
    mean = stats.get("mean", 0)
    median = stats.get("median", 0)
    std_dev = stats.get("stdDev", 0)
    unique_count = stats.get("uniqueCount", 0)
    null_count = stats.get("nullCount", 0)
    percentiles = stats.get("percentiles", {})
    p25 = percentiles.get("p25", 0)
    p50 = percentiles.get("p50", 0)
    p75 = percentiles.get("p75", 0)

    return [
        min_val,
        max_val,
        mean,
        median,
        std_dev,
        unique_count,
        null_count,
        p25,
        p50,
        p75,
    ]


def compare_percentiles(candidate_stats: dict, root_stats: dict) -> float:
    """
    Calculates the Wasserstein distance between the percentile distributions of two datasets.
    
    Extracts the 25th, 50th, and 75th percentiles from each input dictionary and computes the Wasserstein distance (Earth Mover's Distance) between the resulting distributions.
    
    Args:
        candidate_stats: Dictionary containing percentile statistics for the candidate dataset.
        root_stats: Dictionary containing percentile statistics for the root dataset.
    
    Returns:
        The Wasserstein distance as a float, representing the difference between the two percentile distributions.
    """
    candidate_values = extract_percentiles(candidate_stats)
    root_values = extract_percentiles(root_stats)

    logger.debug("Candidate percentiles: %s", candidate_values)
    logger.debug("Root percentiles: %s", root_values)

    distance = wasserstein_distance(candidate_values, root_values)
    logger.debug("Computed Wasserstein distance for percentiles: %f", distance)
    return distance


def should_compare_fields(candidate_field_id: str, root_field_id: str) -> bool:
    """
    Determines whether two field IDs are similar enough for statistical comparison.
    
    Returns True if the IDs match exactly, or if their last segments (after splitting by "/")
    match exactly or are substrings of each other. Returns False if either ID is empty or None,
    or if no similarity is found.
    """
    logger.debug(
        f"Comparing field IDs - Candidate: '{candidate_field_id}', Root: '{root_field_id}'"
    )

    if not candidate_field_id or not root_field_id:
        logger.debug("One or both field IDs are empty or None")
        return False

    if candidate_field_id == root_field_id:
        logger.debug(f"Field IDs match exactly: '{candidate_field_id}'")
        return True

    try:
        candidate_field_name = candidate_field_id.split("/")[-1].lower()
        root_field_name = root_field_id.split("/")[-1].lower()

        logger.debug(
            f"Extracted field names - Candidate: '{candidate_field_name}', Root: '{root_field_name}'"
        )

        if candidate_field_name == root_field_name:
            logger.debug(f"Field names match exactly: '{candidate_field_name}'")
            return True
        if (
            candidate_field_name in root_field_name
            or root_field_name in candidate_field_name
        ):
            logger.debug(
                f"Field names are similar: {candidate_field_name} and {root_field_name}"
            )
            return True
    except Exception as e:
        logger.error(f"Error comparing field IDs: {e}")

    logger.debug(f"Field IDs are not similar: {candidate_field_id} and {root_field_id}")
    return False


def compare_statistics(
    candidate_stats: dict,
    root_stats: dict,
    candidate_field_id: str = None,
    root_field_id: str = None,
) -> float:
    """
    Compares comprehensive statistical summaries of two distributions using the Wasserstein distance.
    
    If both field IDs are provided and determined to be dissimilar, returns infinity to indicate no valid comparison. Otherwise, extracts a set of statistics from each input and computes the Wasserstein distance between them.
    
    Returns:
        The Wasserstein distance between the two distributions, or float('inf') if the field IDs are dissimilar.
    """
    if candidate_field_id is not None and root_field_id is not None:
        if not should_compare_fields(candidate_field_id, root_field_id):
            logger.debug(
                f"Skipping comparison due to dissimilar field IDs: {candidate_field_id} vs {root_field_id}"
            )
            return float("inf")

    candidate_values = extract_statistics(candidate_stats)
    root_values = extract_statistics(root_stats)

    logger.debug("Candidate statistics: %s", candidate_values)
    logger.debug("Root statistics: %s", root_values)

    distance = wasserstein_distance(candidate_values, root_values)
    logger.debug("Computed Wasserstein distance for all statistics: %f", distance)
    return distance


if __name__ == "__main__":
    pass
