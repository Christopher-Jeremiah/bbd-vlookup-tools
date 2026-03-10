import streamlit as st
import pandas as pd
import io

st.set_page_config(page_title="VLOOKUP DATA - BBD", page_icon="🔗", layout="wide")
st.title("🧹 VLOOKUP Laporan BBD")
st.write("Aplikasi ini secara otomatis melakukan VLOOKUP, validasi silang K_Out vs Branch, dan memisahkan data SPH menjadi dua file siap unduh.")

# ==========================================
# 0. INISIALISASI BRANKAS MEMORI (SESSION STATE)
# ==========================================
# Mengamankan data agar tidak hilang saat layar refresh/klik download
if 'proses_selesai' not in st.session_state:
    st.session_state.proses_selesai = False
if 'df_aman' not in st.session_state:
    st.session_state.df_aman = None
if 'df_review' not in st.session_state:
    st.session_state.df_review = None

# ==========================================
# 1. PINTU MASUK DATA & PENGATURAN FILE
# ==========================================
st.sidebar.header("📂 Upload File Laporan")
file_utama = st.sidebar.file_uploader("1. Unggah Laporan BBD (Utama)", type=["xlsx", "csv"])
file_ref = st.sidebar.file_uploader("2. Unggah List NIP (Referensi)", type=["xlsx", "csv"])

st.sidebar.markdown("---")
st.sidebar.header("⚙️ Pengaturan Pembacaan")

# Variabel untuk menyimpan pilihan pengguna
sheet_utama_pilihan = 0
header_utama_pilihan = 0
sheet_ref_pilihan = 0
header_ref_pilihan = 0

# Jika file Excel diunggah, munculkan menu rahasia untuk memilih Sheet dan Baris
if file_utama is not None and file_utama.name.endswith('.xlsx'):
    xls_utama = pd.ExcelFile(file_utama)
    sheet_utama_pilihan = st.sidebar.selectbox("Laporan BBD: Pilih Sheet", xls_utama.sheet_names)
    header_utama_pilihan = st.sidebar.number_input("Laporan BBD: Baris Header (0 = Paling Atas)", min_value=0, value=0, step=1)

if file_ref is not None and file_ref.name.endswith('.xlsx'):
    xls_ref = pd.ExcelFile(file_ref)
    sheet_ref_pilihan = st.sidebar.selectbox("List NIP: Pilih Sheet", xls_ref.sheet_names)
    header_ref_pilihan = st.sidebar.number_input("List NIP: Baris Header (0 = Paling Atas)", min_value=0, value=0, step=1)

# Fungsi pembaca file yang sudah di-upgrade
def baca_file(file, sheet_pilihan, header_pilihan):
    if file.name.endswith('.csv'):
        # CSV tidak punya sheet, tapi bisa diatur headernya
        return pd.read_csv(file, header=header_pilihan, dtype=str)
    else:
        # Excel bisa diatur sheet dan headernya
        return pd.read_excel(file, sheet_name=sheet_pilihan, header=header_pilihan, dtype=str)

# ==========================================
# 2. MESIN PENGOLAHAN (PIPELINE)
# ==========================================
if file_utama is not None and file_ref is not None:
    # Tombol ini sekarang hanya bertugas menyalakan mesin dan mengisi brankas
    if st.button("🚀 Proses & Pisahkan Data Sekarang!", use_container_width=True):
        with st.spinner('Sedang memproses ribuan baris data...'):
            try:
                df_main = baca_file(file_utama, sheet_utama_pilihan, header_utama_pilihan)
                df_ref = baca_file(file_ref, sheet_ref_pilihan, header_ref_pilihan)
                
                # ==========================================
                # MENGHAPUS BARIS TOTAL (PEMURNIAN DATA)
                # ==========================================
                # 1. Bersihkan NIP dari spasi, dan pastikan yang kosong jadi teks kosong ''
                df_main['NIP RM'] = df_main['NIP RM'].fillna('').astype(str).str.strip()
                df_ref['NIP'] = df_ref['NIP'].fillna('').astype(str).str.strip()
                
                # 2. TEBAS BARIS TOTAL: Perintahkan Pandas untuk hanya menyimpan baris yang NIP-nya ADA ISINYA
                # (Saring baris yang tidak sama dengan '' dan tidak sama dengan teks 'nan')
                df_main = df_main[(df_main['NIP RM'] != '') & (df_main['NIP RM'].str.lower() != 'nan')]
                df_ref = df_ref[(df_ref['NIP'] != '') & (df_ref['NIP'].str.lower() != 'nan')]
                
                kamus_k_out = dict(zip(df_ref['NIP'], df_ref['K_Outlet']))
                kamus_rmcode = dict(zip(df_ref['NIP'], df_ref['RMCode']))
                # ... (lanjutkan dengan kode VLOOKUP Anda di bawahnya)
                
                df_main['K_Out by NIP'] = df_main['NIP RM'].map(kamus_k_out)
                df_main['RMCode'] = df_main['NIP RM'].map(kamus_rmcode)
                
                df_main['K_Out by NIP'] = df_main['K_Out by NIP'].fillna('')
                df_main['BRANCH'] = df_main['BRANCH'].fillna('')
                
                df_main['<<cek>>'] = (df_main['K_Out by NIP'] != df_main['BRANCH']).astype(int).astype(str)
                
                df_main.columns = df_main.columns.astype(str)
                df_main.columns = df_main.columns.astype(str)
                
                # ==========================================
                # PERMAK JUDUL KOLOM TANGGAL
                # ==========================================
                kolom_rapi = []
                for col in df_main.columns:
                    if ' 00:00:00' in col:
                        tgl_obj = pd.to_datetime(col)
                        kolom_rapi.append(tgl_obj.strftime('%d-%b'))
                    else:
                        kolom_rapi.append(col)
                df_main.columns = kolom_rapi
                
                # ... (kode nama_kolom = list(df_main.columns) lanjut di bawahnya)
                nama_kolom = list(df_main.columns)
                kolom_account_asli = [col for col in nama_kolom if col.upper() == 'ACCOUNT'][0]
                
                nama_kolom.remove('RMCode')
                nama_kolom.remove('<<cek>>')
                nama_kolom.remove('K_Out by NIP')
                
                indeks_account = nama_kolom.index(kolom_account_asli) + 1
                nama_kolom.insert(indeks_account, 'RMCode')
                nama_kolom.insert(indeks_account + 1, '<<cek>>')
                nama_kolom.insert(indeks_account + 2, 'K_Out by NIP')
                
                df_main = df_main[nama_kolom]
                # Ganti tulisan 'TANGGAL' dengan nama judul kolom yang sebenarnya di file Excel Anda
                df_main['TGL REAL'] = df_main['TGL REAL'].str.replace(' 00:00:00', '', regex=False)
                df_main['TGL REAL'] = pd.to_datetime(df_main['TGL REAL']).dt.date

                # ==========================================
                # MENGEMBALIKAN WUJUD ANGKA (AUTO-DETECT NUMBERS)
                # ==========================================
                # 1. Daftar laci yang HARUS DILINDUNGI agar tetap berupa Teks murni
                kolom_teks_mutlak = ['GP1PDT', 'BRANCH', 'ACCOUNT', 'RMCode', '<<cek>>', 'K_Out by NIP', 'CIF', 'NIP RM', 'TGL REAL']
                
                # 2. Mesin berkeliling ke seluruh kolom
                for col in df_main.columns:
                    if col not in kolom_teks_mutlak:
                        df_main[col] = pd.to_numeric(df_main[col], errors='ignore')
                
                baris_sph = df_main['RMCode'].fillna('').str.contains('SPH', case=False)
                baris_cek_1 = df_main['<<cek>>'] == '1'
                kondisi_anomali = baris_cek_1 | baris_sph
                
            
               # MASUKKAN HASIL KE DALAM BRANKAS MEMORI
                st.session_state.df_aman = df_main[~kondisi_anomali]
                
                # ==========================================
                # SORTING BERTINGKAT UNTUK FILE 2 (REVIEW)
                # ==========================================
                df_rev = df_main[kondisi_anomali].copy()
                
                # Buat kolom radar bantuan untuk mendeteksi sel kosong
                df_rev['K_Out_Kosong'] = df_rev['K_Out by NIP'] == ''
                
                # Lakukan pengurutan 3 tahap
                df_rev = df_rev.sort_values(
                    by=['<<cek>>', 'K_Out_Kosong', 'K_Out by NIP'], 
                    ascending=[True, True, True]
                )
                
                # Hapus kembali kolom radar bantuan agar tidak ikut ter-download
                df_rev = df_rev.drop(columns=['K_Out_Kosong'])
                
                # Masukkan hasil rapi ini ke dalam brankas utama
                st.session_state.df_review = df_rev
                st.session_state.df_review.loc[st.session_state.df_review['<<cek>>'] == '1', 'RMCode'] = '' 
                st.session_state.proses_selesai = True # Kunci brankas ditandai "Ada Isinya"
                
            except Exception as e:
                st.error(f"❌ Terjadi kesalahan: {e}")

    # ==========================================
    # 3. AREA UNDUH DATA (MENGAMBIL DARI BRANKAS)
    # ==========================================
    # Bagian ini letaknya sejajar (di luar) tombol Proses.
    # Selama brankas ada isinya (True), tombol Download akan terus muncul!
    if st.session_state.proses_selesai:
        st.success("✅ Proses Selesai! Data berhasil dipisahkan berdasarkan kesesuaian cabang dan RMCode.")
        st.markdown("---")
        st.subheader("📥 Unduh Hasil Pemrosesan")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.info(f"📁 **File 1 (Data Clean)**\nBerisi {len(st.session_state.df_aman)} baris data selaras (0).")
            buffer_aman = io.BytesIO()
            
            # MENGGUNAKAN MESIN EXCELWRITER UNTUK MEMAKSA FORMAT TANGGAL
            with pd.ExcelWriter(buffer_aman, engine='openpyxl', date_format='DD-MM-YYYY', datetime_format='DD-MM-YYYY') as writer:
                st.session_state.df_aman.to_excel(writer, index=False)
                
            st.download_button(
                label="📊 Download File 1 (Clean)",
                data=buffer_aman.getvalue(),
                file_name="File BBD RMCode 0.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_btn_1"
            )
        
        with col2:
            st.warning(f"⚠️ **File 2 (Data Anomali)**\nBerisi {len(st.session_state.df_review)} baris data selisih (1) & RMCode SPH.")
            buffer_review = io.BytesIO()
            
            # MENGGUNAKAN MESIN EXCELWRITER UNTUK MEMAKSA FORMAT TANGGAL
            with pd.ExcelWriter(buffer_review, engine='openpyxl', date_format='DD-MM-YYYY', datetime_format='DD-MM-YYYY') as writer:
                st.session_state.df_review.to_excel(writer, index=False)
                
            st.download_button(
                label="📊 Download File 2 (Review/SPH)",
                data=buffer_review.getvalue(),
                file_name="File BBD RMCode 1.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_btn_2"
            )

else:
    st.info("👈 Silakan unggah **Laporan BBD** dan **List NIP** di menu sebelah kiri terlebih dahulu.")
