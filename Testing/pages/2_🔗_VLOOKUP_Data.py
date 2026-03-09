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
# 1. PINTU MASUK DATA
# ==========================================
st.sidebar.header("📂 Upload File Laporan")
file_utama = st.sidebar.file_uploader("1. Unggah Laporan BBD (Utama)", type=["xlsx", "csv"])
file_ref = st.sidebar.file_uploader("2. Unggah List NIP (Referensi)", type=["xlsx", "csv"])

def baca_file(file):
    if file.name.endswith('.csv'):
        return pd.read_csv(file, dtype=str)
    else:
        return pd.read_excel(file, dtype=str)

# ==========================================
# 2. MESIN PENGOLAHAN (PIPELINE)
# ==========================================
if file_utama is not None and file_ref is not None:
    # Tombol ini sekarang hanya bertugas menyalakan mesin dan mengisi brankas
    if st.button("🚀 Proses & Pisahkan Data Sekarang!", use_container_width=True):
        with st.spinner('Sedang memproses ribuan baris data...'):
            try:
                df_main = baca_file(file_utama)
                df_ref = baca_file(file_ref)
                
                df_main['NIP RM'] = df_main['NIP RM'].str.strip()
                df_ref['NIP'] = df_ref['NIP'].str.strip()
                
                kamus_k_out = dict(zip(df_ref['NIP'], df_ref['K_Outlet']))
                kamus_rmcode = dict(zip(df_ref['NIP'], df_ref['RMCode']))
                
                df_main['K_Out by NIP'] = df_main['NIP RM'].map(kamus_k_out)
                df_main['RMCode'] = df_main['NIP RM'].map(kamus_rmcode)
                
                df_main['K_Out by NIP'] = df_main['K_Out by NIP'].fillna('')
                df_main['BRANCH'] = df_main['BRANCH'].fillna('')
                
                df_main['<<cek>>'] = (df_main['K_Out by NIP'] != df_main['BRANCH']).astype(int).astype(str)
                
                df_main.columns = df_main.columns.astype(str)
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
                
                # 🧹 Sapu bersih jam tengah malam di seluruh tabel sekaligus
                df_main = df_main.replace(' 00:00:00', '', regex=False)
                
                baris_sph = df_main['RMCode'].fillna('').str.contains('SPH', case=False)
                baris_cek_1 = df_main['<<cek>>'] == '1'
                kondisi_anomali = baris_cek_1 | baris_sph
                
                # ==========================================
                # MASUKKAN HASIL KE DALAM BRANKAS MEMORI
                # ==========================================
                # File 1: Brankas Aman (Gunakan .copy() agar mandiri)
                df_aman_temp = df_main[~kondisi_anomali].copy()
                df_aman_temp = df_aman_temp.sort_values(by='K_Out by NIP') # Diurutkan agar seragam seperti Excel
                st.session_state.df_aman = df_aman_temp
                
                # File 2: Brankas Review
                df_review_temp = df_main[kondisi_anomali].copy()
                
                # 🎯 TEMBAKAN SUPER AKURAT: Pastikan wujudnya teks saat dicocokkan
                df_review_temp.loc[df_review_temp['<<cek>>'].astype(str) == '1', 'RMCode'] = ''
                
                # ⚡ CARA 1 BARIS (Jalan Pintas Pandas): Mengurutkan data dan menendang baris kosong ke paling bawah
                df_review_temp = df_review_temp.replace('', None).sort_values(by=['<<cek>>', 'K_Out by NIP'], na_position='last').fillna('')
                
                # Masukkan tabel yang sudah bersih ke dalam brankas
                st.session_state.df_review = df_review_temp
                st.session_state.proses_selesai = True 
                
            except Exception as e:
                st.error(f"❌ Terjadi kesalahan: {e}")

    # ==========================================
    # 3. AREA UNDUH DATA (MENGAMBIL DARI BRANKAS)
    # ==========================================
    if st.session_state.proses_selesai:
        st.success("✅ Proses Selesai! Data berhasil dipisahkan berdasarkan kesesuaian cabang dan RMCode.")
        st.markdown("---")
        st.subheader("📥 Unduh Hasil Pemrosesan")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.info(f"📁 **File 1 (Data Clean)**\nBerisi {len(st.session_state.df_aman)} baris data selaras (0).")
            buffer_aman = io.BytesIO()
            st.session_state.df_aman.to_excel(buffer_aman, index=False, engine='openpyxl')
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
            st.session_state.df_review.to_excel(buffer_review, index=False, engine='openpyxl')
            st.download_button(
                label="📊 Download File 2 (Review/SPH)",
                data=buffer_review.getvalue(),
                file_name="File BBD RMCode 1.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_btn_2"
            )

else:
    st.info("👈 Silakan unggah **Laporan BBD** dan **List NIP** di menu sebelah kiri terlebih dahulu.")