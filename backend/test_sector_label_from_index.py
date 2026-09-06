from backend.services.sector_label_from_index import sector_label_from_index


def test_fin_service_exact():
    assert sector_label_from_index("NSE_INDEX|Nifty Fin Service") == "Fin Service"


def test_common_shorts():
    assert sector_label_from_index("NSE_INDEX|Nifty IT") == "IT"
    assert sector_label_from_index("NSE_INDEX|Nifty Bank") == "Bank"
    assert sector_label_from_index("NSE_INDEX|Nifty Realty") == "Realty"
    assert sector_label_from_index("NSE_INDEX|Nifty Auto") == "Auto"
    assert sector_label_from_index("NSE_INDEX|NIFTY HEALTHCARE") == "HEALTHCARE"
    assert sector_label_from_index("NSE_INDEX|NIFTY CONSR DURBL") == "CONSR DURBL"
    assert sector_label_from_index("NSE_INDEX|Nifty Pvt Bank") == "Pvt Bank"
