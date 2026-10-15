"""Correspondance entre les codes publics Arcep et le vocabulaire du modele."""

from ftth.config import trimestre_index, trimestre_label
from ftth.data_acquisition import classify_zone, normalize_imb_etat


def test_calendrier_inclusif_2018_t2_2026_t2():
    assert trimestre_label(1) == "2018_T2"
    assert trimestre_index(2018, 2) == 1
    assert trimestre_label(32) == "2026_T1"
    assert trimestre_index(2026, 2) == 33
    assert trimestre_label(33) == "2026_T2"


def test_etats_ipe_vers_chaine_du_modele():
    assert normalize_imb_etat("cible") == "etudes"
    assert normalize_imb_etat("signe") == "programme"
    assert normalize_imb_etat("en cours de deploiement") == "en_cours"
    assert normalize_imb_etat("raccordable demande") == "raccordable_demande"
    assert normalize_imb_etat("rad en cours de deploiement") == "raccordable_demande"
    assert normalize_imb_etat("deploye") == "raccordable"
    assert normalize_imb_etat("  Deploye ") == "raccordable"
    assert normalize_imb_etat("abandonne") == "abandonne"


def test_archive_choisit_le_csv_immeubles_et_pas_les_operateurs():
    import io
    import zipfile
    from pathlib import Path
    from ftth.data_acquisition import _zip_csv_member

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("2020T1_Immeuble/codes_operateur.csv", "code_l33;nom_operateur\nFRTE;Orange\n")
        archive.writestr("2020T1_Immeuble/mv_immeubles_2020t1.csv", "x,y,imb_id\n1,2,A\n" * 50)
    path = Path("/tmp/multi_csv_2020t1.zip")
    path.write_bytes(buffer.getvalue())
    assert _zip_csv_member(path).endswith("mv_immeubles_2020t1.csv")


def test_zonage_communal_cartefibre():
    assert classify_zone("ZTD", "0", 0, 1) == "A_ZTD"
    assert classify_zone("zipri", "Orange", 0, 0) == "B_AMII"
    assert classify_zone("zipri", "SFR", 0, 0) == "B_AMII"
    assert classify_zone("zipri", "0", 4, 0) == "B_AMEL"
    assert classify_zone("zipu", "0", 0, 0) == "C_RIP"
    assert classify_zone("zipu", "0", 3, 0) == "B_AMEL"
