import streamlit as st
import pandas as pd
import numpy as np
import io

st.set_page_config(page_title="Otomatisasi BBD", page_icon="⚙️", layout="wide")
st.title("⚙️ Otomatisasi BBD")
st.write("Aplikasi ini dirancang khusus untuk memproses data BBD. Anda wajib mengunggah File Utama dan Kamus 2 (List NIP). Kamus 1 (BBD Kemarin) bersifat opsional.")

# ==========================================
# 0. INISIALISASI BRANKAS MEMORI
# ==========================================
if 'proses_selesai' not in st.session_state:
    st.session_state.proses_selesai = False
if 'tabel_1' not in st.session_state:
    st.session_state.tabel_1 = None
if 'tabel_2' not in st.session_state:
    st.session_state.tabel_2 = None
if 'tabel_3a' not in st.session_state:
    st.session_state.tabel_3a = None
if 'tabel_3b' not in st.session_state:
    st.session_state.tabel_3b = None
if 'pakai_kamus_1' not in st.session_state:
    st.session_state.pakai_kamus_1 = False

# ==========================================
# 1. PINTU MASUK DATA & PENGATURAN UI DINAMIS
# ==========================================
st.sidebar.header("📂 Upload File")
file_utama = st.sidebar.file_uploader("1. File Utama (BBD Hari Ini) *[Wajib]*", type=["xlsx", "csv"])
file_kamus1 = st.sidebar.file_uploader("2. Kamus 1 (BBD Kemarin) *[Opsional]*", type=["xlsx", "csv"])
file_kamus2 = st.sidebar.file_uploader("3. Kamus 2 (List NIP) *[Wajib]*", type=["xlsx", "csv"])

st.sidebar.markdown("---")
st.sidebar.header("⚙️ Pengaturan Pembacaan")

if file_utama is not None:
    if file_utama.name.endswith('.xlsx'):
        xls_u = pd.ExcelFile(file_utama)
        sheet_u = st.sidebar.selectbox("File Utama: Pilih Sheet", xls_u.sheet_names)
    else:
        sheet_u = 0
    header_u = st.sidebar.number_input("File Utama: Baris Header (Mulai 0)", min_value=0, value=0, step=1)
else:
    sheet_u = 0
    header_u = 1

if file_kamus1 is not None:
    if file_kamus1.name.endswith('.xlsx'):
        xls_k1 = pd.ExcelFile(file_kamus1)
        sheet_k1 = st.sidebar.selectbox("Kamus 1: Pilih Sheet", xls_k1.sheet_names)
    else:
        sheet_k1 = 0
    header_k1 = st.sidebar.number_input("Kamus 1: Baris Header (Mulai 0)", min_value=0, value=1, step=1)
else:
    sheet_k1 = 0
    header_k1 = 1

if file_kamus2 is not None:
    if file_kamus2.name.endswith('.xlsx'):
        xls_k2 = pd.ExcelFile(file_kamus2)
        sheet_k2 = st.sidebar.selectbox("Kamus 2: Pilih Sheet", xls_k2.sheet_names)
    else:
        sheet_k2 = 0
    header_k2 = st.sidebar.number_input("Kamus 2: Baris Header (Mulai 0)", min_value=0, value=0, step=1)
else:
    sheet_k2 = 0
    header_k2 = 0

def baca_file(file, sheet, header):
    if file.name.endswith('.csv'):
        return pd.read_csv(file, header=header, dtype=str)
    else:
        return pd.read_excel(file, sheet_name=sheet, header=header, dtype=str)

# ==========================================
# 2. MESIN PENGOLAHAN (CASCADING ETL)
# ==========================================
if file_utama is not None and file_kamus2 is not None:
    if st.button("🚀 Eksekusi Proses Data Sekarang!", use_container_width=True):
        with st.spinner("Mesin sedang memproses..."):
            try:
                st.session_state.pakai_kamus_1 = (file_kamus1 is not None)
                
                df_u = baca_file(file_utama, sheet_u, header_u)
                df_k2 = baca_file(file_kamus2, sheet_k2, header_k2)
                
                def perbaiki_judul_tanggal(df):
                    kolom_rapi = []
                    for col in df.columns:
                        if isinstance(col, pd.Timestamp):
                            kolom_rapi.append(col.strftime('%d-%b'))
                        else:
                            col_str = str(col)
                            if '00:00:00' in col_str or (len(col_str) >= 10 and col_str[4] == '-' and col_str[7] == '-'):
                                try:
                                    tgl_obj = pd.to_datetime(col_str)
                                    kolom_rapi.append(tgl_obj.strftime('%d-%b'))
                                except:
                                    kolom_rapi.append(col_str)
                            else:
                                kolom_rapi.append(col_str)
                    df.columns = kolom_rapi
                    return df

                df_u = perbaiki_judul_tanggal(df_u)
                
                if 'TGL REAL' in df_u.columns:
                    df_u['TGL REAL'] = pd.to_datetime(df_u['TGL REAL'], errors='coerce').dt.strftime('%d-%m-%Y').fillna('')

                # =========================================================
                # PERUBAHAN: Menggunakan GP1PDT sebagai pembersih Grand Total
                # =========================================================
                if 'GP1PDT' in df_u.columns:
                    df_u['GP1PDT'] = df_u['GP1PDT'].fillna('').astype(str).str.strip()
                    # Buang baris yang GP1PDT-nya kosong atau ada kata 'total'
                    df_u = df_u[(df_u['GP1PDT'] != '') & (df_u['GP1PDT'].str.lower() != 'nan') & (~df_u['GP1PDT'].str.lower().str.contains('total'))]

                if 'NIP RM' in df_u.columns:
                    # NIP RM tetap dibersihkan format spasinya, TAPI TIDAK ADA LAGI baris yang dihapus
                    df_u['NIP RM'] = df_u['NIP RM'].fillna('').astype(str).str.strip()
                # =========================================================
                
                df_u['ACCOUNT'] = df_u['ACCOUNT'].fillna('').astype(str).str.strip()
                
                df_k2['NIP'] = df_k2['NIP'].fillna('').astype(str).str.strip()
                df_k2['K_Outlet'] = df_k2['K_Outlet'].fillna('').astype(str).str.strip()
                df_k2['Outlet'] = df_k2['Outlet'].fillna('').astype(str).str.strip().str.upper()
                if 'Kantor Cabang' in df_k2.columns:
                    df_k2['Kantor Cabang'] = df_k2['Kantor Cabang'].fillna('').astype(str).str.strip().str.upper()
                df_k2['Jabatan'] = df_k2['Jabatan'].fillna('').astype(str).str.strip().str.upper()

                # =========================================================
                # PROSES 1: MENGGUNAKAN DATA KEMARIN (JIKA ADA)
                # =========================================================
                if 'KONVERSI KPP' in df_u.columns:
                    dict_replace_kpp = {
                        "KPP Demand - Konversi Konsumer": "Konversi Konsumer",
                        "KPP Demand Konversi KPR - CP RM": "Konversi Konsumer",
                        "KPP Demand - Kompensasi Konsumer": "Konversi Konsumer"
                    }
                    df_u['KONVERSI KPP'] = df_u['KONVERSI KPP'].replace(dict_replace_kpp)
                
                if 'RMCode' not in df_u.columns:
                    df_u['RMCode'] = np.nan
                
                if st.session_state.pakai_kamus_1:
                    df_k1 = baca_file(file_kamus1, sheet_k1, header_k1)
                    df_k1['ACCOUNT'] = df_k1['ACCOUNT'].fillna('').astype(str).str.strip()
                    kamus_1_rmcode = df_k1.drop_duplicates(subset=['ACCOUNT']).set_index('ACCOUNT')['RMCode'].to_dict()
                    df_u['RMCode'] = df_u['ACCOUNT'].map(kamus_1_rmcode)
                    
                if 'SNAME' in df_u.columns:
                    cols = list(df_u.columns)
                    if 'RMCode' in cols:
                        cols.remove('RMCode')
                    idx_sname = cols.index('SNAME')
                    cols.insert(idx_sname + 1, 'RMCode')
                    df_u = df_u[cols]
                
                mask_match_acc = df_u['RMCode'].notna() & (df_u['RMCode'].astype(str).str.strip() != '') & (df_u['RMCode'].astype(str).str.strip().str.lower() != 'nan')
                tabel_1 = df_u[mask_match_acc].copy()
                tabel_2 = df_u[~mask_match_acc].copy()

                # =========================================================
                # PROSES 2: VALIDASI NIP NORMAL PADA TABEL 2
                # =========================================================
                if not tabel_2.empty:
                    kamus_2_kout = df_k2.drop_duplicates(subset=['NIP']).set_index('NIP')['K_Outlet'].to_dict()
                    tabel_2['K_Out by NIP'] = tabel_2['NIP RM'].map(kamus_2_kout).fillna('')
                    
                    tabel_2['BRANCH'] = tabel_2['BRANCH'].fillna('').astype(str).str.strip()
                    tabel_2['<<cek>>'] = np.where(tabel_2['K_Out by NIP'] == tabel_2['BRANCH'], '0', '1')
                    
                    kamus_2_rmcode = df_k2.drop_duplicates(subset=['NIP']).set_index('NIP')['RMCode'].to_dict()
                    mask_cek_0 = tabel_2['<<cek>>'] == '0'
                    
                    tabel_2['RMCode'] = tabel_2['RMCode'].astype(object) # FIX PANDAS WARNING
                    tabel_2.loc[mask_cek_0, 'RMCode'] = tabel_2.loc[mask_cek_0, 'NIP RM'].map(kamus_2_rmcode)
                    
                    cols_t2 = list(tabel_2.columns)
                    cols_t2.remove('<<cek>>')
                    cols_t2.remove('K_Out by NIP')
                    if 'RMCode' in cols_t2:
                        idx_rmcode = cols_t2.index('RMCode')
                        cols_t2.insert(idx_rmcode + 1, '<<cek>>')
                        cols_t2.insert(idx_rmcode + 2, 'K_Out by NIP')
                    tabel_2 = tabel_2[cols_t2]
                    
                    mask_cek_1 = tabel_2['<<cek>>'] == '1'
                    mask_sph = tabel_2['RMCode'].fillna('').astype(str).str.contains('SPH', case=False)
                    
                    tabel_3 = tabel_2[mask_cek_1 | mask_sph].copy()
                    tabel_2_clean = tabel_2[~(mask_cek_1 | mask_sph)].copy()
                else:
                    tabel_3 = pd.DataFrame()
                    tabel_2_clean = pd.DataFrame()

                # =========================================================
                # PROSES 3: VLOOKUP BERSYARAT (5-TIER FALLBACK) PADA TABEL 3
                # =========================================================
                if not tabel_3.empty:
                    df_k2_cps_exact = df_k2[df_k2['Jabatan'] == 'CPS']
                    
                    df_k2_cps_contains = df_k2[
                        df_k2['Jabatan'].str.contains('CPS', case=False, na=False) & 
                        (df_k2['Jabatan'] != 'CPS') & 
                        (~df_k2['Jabatan'].str.contains('#', na=False))
                    ]
                    
                    kamus_branch_kout_cps = df_k2_cps_exact.drop_duplicates(subset=['K_Outlet']).set_index('K_Outlet')['RMCode'].to_dict()
                    kamus_kc_outlet_cps = df_k2_cps_exact.drop_duplicates(subset=['Outlet']).set_index('Outlet')['RMCode'].to_dict()
                    kamus_kc_outlet_smecps = df_k2_cps_contains.drop_duplicates(subset=['Outlet']).set_index('Outlet')['RMCode'].to_dict()
                    kamus_kc_kancab_cps = df_k2_cps_exact.drop_duplicates(subset=['Kantor Cabang']).set_index('Kantor Cabang')['RMCode'].to_dict()
                    kamus_kc_kancab_smecps = df_k2_cps_contains.drop_duplicates(subset=['Kantor Cabang']).set_index('Kantor Cabang')['RMCode'].to_dict()
                    
                    tabel_3['BRANCH'] = tabel_3['BRANCH'].fillna('').astype(str).str.strip()
                    tabel_3['KC'] = tabel_3['KC'].fillna('').astype(str).str.strip().str.upper()
                    
                    def cek_sel_kosong(df):
                        return df['RMCode'].isna() | (df['RMCode'].astype(str).str.strip() == '') | (df['RMCode'].astype(str).str.strip().str.lower() == 'nan')

                    tabel_3['RMCode'] = tabel_3['BRANCH'].map(kamus_branch_kout_cps)
                    
                    mask_kosong = cek_sel_kosong(tabel_3)
                    tabel_3.loc[mask_kosong, 'RMCode'] = tabel_3.loc[mask_kosong, 'KC'].map(kamus_kc_outlet_cps)

                    mask_kosong = cek_sel_kosong(tabel_3)
                    tabel_3.loc[mask_kosong, 'RMCode'] = tabel_3.loc[mask_kosong, 'KC'].map(kamus_kc_outlet_smecps)

                    mask_kosong = cek_sel_kosong(tabel_3)
                    tabel_3.loc[mask_kosong, 'RMCode'] = tabel_3.loc[mask_kosong, 'KC'].map(kamus_kc_kancab_cps)

                    mask_kosong = cek_sel_kosong(tabel_3)
                    tabel_3.loc[mask_kosong, 'RMCode'] = tabel_3.loc[mask_kosong, 'KC'].map(kamus_kc_kancab_smecps)

                    if 'KONVERSI KPP' in tabel_3.columns:
                        kpp_str = tabel_3['KONVERSI KPP'].fillna('').astype(str).str.strip().str.lower()
                        mask_bukan_konversi = (kpp_str != 'konversi konsumer')
                    else:
                        mask_bukan_konversi = pd.Series(True, index=tabel_3.index)
                        
                    tabel_3b = tabel_3[mask_bukan_konversi].copy()
                    tabel_3a = tabel_3[~mask_bukan_konversi].copy()
                    
                    tabel_3b['RMCode'] = np.nan
                else:
                    tabel_3a = pd.DataFrame()
                    tabel_3b = pd.DataFrame()

                def urutkan_rmcode(df):
                    if not df.empty and 'RMCode' in df.columns:
                        return df.sort_values(by='RMCode', ascending=True, na_position='last')
                    return df

                tabel_1 = urutkan_rmcode(tabel_1)
                tabel_2_clean = urutkan_rmcode(tabel_2_clean)
                tabel_3a = urutkan_rmcode(tabel_3a)
                tabel_3b = urutkan_rmcode(tabel_3b)

                st.session_state.tabel_1 = tabel_1
                st.session_state.tabel_2 = tabel_2_clean
                st.session_state.tabel_3a = tabel_3a
                st.session_state.tabel_3b = tabel_3b
                st.session_state.proses_selesai = True
                
            except Exception as e:
                import traceback
                st.error(f"❌ Terjadi kesalahan mesin: {e}. Detail: {traceback.format_exc()}")

# ==========================================
# 3. AREA UNDUH DATA (ADAPTIF UI)
# ==========================================
if st.session_state.proses_selesai:
    st.success("✅ Seluruh tahapan Cascading ETL, Kosmetik Tanggal, dan Pengurutan A-Z berhasil dieksekusi!")
    st.markdown("---")
    
    def df_to_excel(df):
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl', date_format='DD-MM-YYYY', datetime_format='DD-MM-YYYY') as writer:
            df.to_excel(writer, index=False)
        return buffer.getvalue()

    if st.session_state.pakai_kamus_1:
        st.subheader("📥 Unduh Hasil Pemrosesan (4 File Terpisah)")
        col1, col2 = st.columns(2)
        col3, col4 = st.columns(2)
        
        with col1:
            st.info(f"📁 **Tabel 1 (Cocok Kamus H-1)**\nBerisi {len(st.session_state.tabel_1)} baris data.")
            if not st.session_state.tabel_1.empty:
                st.download_button("⬇️ Download Tabel 1", data=df_to_excel(st.session_state.tabel_1), file_name="1_Tabel_Match_Kemarin.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        with col2:
            st.success(f"📁 **Tabel 2 (Cocok NIP - Clean)**\nBerisi {len(st.session_state.tabel_2)} baris data.")
            if not st.session_state.tabel_2.empty:
                st.download_button("⬇️ Download Tabel 2", data=df_to_excel(st.session_state.tabel_2), file_name="2_Tabel_Match_NIP.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            
        with col3:
            st.warning(f"⚠️ **Tabel 3A (Anomali - Konversi Konsumer)**\nBerisi {len(st.session_state.tabel_3a)} baris data.")
            if not st.session_state.tabel_3a.empty:
                st.download_button("⬇️ Download Tabel 3A", data=df_to_excel(st.session_state.tabel_3a), file_name="3A_Tabel_Anomali_Konversi_Konsumer.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            
        with col4:
            st.error(f"🛑 **Tabel 3B (Anomali - Bukan Konv. Konsumer)**\nBerisi {len(st.session_state.tabel_3b)} baris data.")
            if not st.session_state.tabel_3b.empty:
                st.download_button("⬇️ Download Tabel 3B", data=df_to_excel(st.session_state.tabel_3b), file_name="3B_Tabel_Anomali_Bukan_Konversi.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    else:
        st.subheader("📥 Unduh Hasil Pemrosesan (3 File Terpisah)")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.success(f"📁 **Data Valid (NIP & Cabang Cocok)**\nBerisi {len(st.session_state.tabel_2)} baris data.")
            if not st.session_state.tabel_2.empty:
                st.download_button("⬇️ Download Data Valid", data=df_to_excel(st.session_state.tabel_2), file_name="1_Data_Valid_NIP.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                
        with col2:
            st.warning(f"⚠️ **Data Anomali (Konversi Konsumer)**\nBerisi {len(st.session_state.tabel_3a)} baris data.")
            if not st.session_state.tabel_3a.empty:
                st.download_button("⬇️ Download Anomali Konversi", data=df_to_excel(st.session_state.tabel_3a), file_name="2_Anomali_Konversi_Konsumer.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                
        with col3:
            st.error(f"🛑 **Data Anomali (Bukan Konv. Konsumer)**\nBerisi {len(st.session_state.tabel_3b)} baris data.")
            if not st.session_state.tabel_3b.empty:
                st.download_button("⬇️ Download Anomali Bukan Konv.", data=df_to_excel(st.session_state.tabel_3b), file_name="3_Anomali_Bukan_Konversi.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

elif not file_utama or not file_kamus2:
    st.info("👈 Silakan unggah minimal File Utama dan Kamus 2 (List NIP) di menu sebelah kiri terlebih dahulu.")
