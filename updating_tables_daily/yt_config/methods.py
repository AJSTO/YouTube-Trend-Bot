import re


def convert_duration_to_seconds(duration):
    """
    Converts a duration string to the total duration in seconds.

    Args:
        duration (str): Duration string in the format 'PT<hours>H<minutes>M<seconds>S'.

    Returns:
        int: Total duration in seconds.

    Examples:
        >>> convert_duration_to_seconds('PT1H30M15S')
        5415
        >>> convert_duration_to_seconds('PT10M')
        600
        >>> convert_duration_to_seconds('PT45S')
        45
    """
    duration = duration[2:]  # Remove the 'PT' prefix

    # Extract hours, minutes, and seconds using regular expressions
    hours = int(re.findall(r'(\d+)H', duration)[0]) if 'H' in duration else 0
    minutes = int(re.findall(r'(\d+)M', duration)[0]) if 'M' in duration else 0
    seconds = int(re.findall(r'(\d+)S', duration)[0]) if 'S' in duration else 0

    total_seconds = hours * 3600 + minutes * 60 + seconds
    return total_seconds