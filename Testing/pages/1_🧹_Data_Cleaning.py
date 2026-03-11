import streamlit as st
import pandas as pd
import io

# ==========================================
# PENGATURAN HALAMAN UTAMA
# ==========================================
st.set_page_config(page_title="Dashboard SDD - Cleaning & VLOOKUP", layout="wide")
st.title("🛠️ Dashboard SDD: Data Cleaning & VLOOKUP")

st.sidebar.header("📂 1. Upload Laporan Utama")
file_utama = st.sidebar.file_uploader("Unggah File Utama", type=["xlsx", "csv"])

st.sidebar.header("📂 2. Upload Data Referensi")
file_referensi = st.sidebar.file_uploader("Unggah File Referensi (Untuk VLOOKUP)", type=["xlsx", "csv"])

# ==========================================
# FUNGSI PEMBACA FILE (CSV / EXCEL)
# ==========================================
# Membuat fungsi kecil agar kita tidak mengetik ulang kode yang sama
def baca_file(file_unggahan):
    nama_file = file_unggahan.name
    if nama_file.endswith('.csv'):
        baris_dilewati = st.sidebar.slider(f"Potong baris dari atas ({nama_file}):", 0, 30, 0)
        df = pd.read_csv(file_unggahan, skiprows=baris_dilewati)
    else:
        xls = pd.ExcelFile(file_unggahan)
        sheet_pilihan = st.sidebar.selectbox(f"Pilih Sheet ({nama_file}):", xls.sheet_names)
        baris_dilewati = st.sidebar.slider(f"Potong baris dari atas ({nama_file}):", 0, 30, 0)
        df = pd.read_excel(file_unggahan, sheet_name=sheet_pilihan, skiprows=baris_dilewati)
    return df

# ==========================================
# JIKA FILE UTAMA SUDAH MASUK
# ==========================================
if file_utama is not None:
    st.subheader("Fase 1: Pembersihan File Utama")
    df_mentah = baca_file(file_utama)
    
    # Pembersihan Dasar
    df_bersih = df_mentah.dropna(how='all').dropna(axis=1, how='all')
    st.dataframe(df_bersih)
    
    # ==========================================
    # JIKA FILE REFERENSI JUGA SUDAH MASUK (MULAI VLOOKUP)
    # ==========================================
    if file_referensi is not None:
        st.markdown("---")
        st.subheader("Fase 2: Proses VLOOKUP (Merge) dengan File Referensi")
        df_ref = baca_file(file_referensi)
        
        # Menampilkan data referensi sejenak agar user bisa lihat
        with st.expander("Klik untuk mengintip isi Data Referensi"):
            st.dataframe(df_ref)
            
        # Membuat Dropdown untuk memilih kolom VLOOKUP
        st.write("### ⚙️ Pengaturan VLOOKUP")
        kolom1, kolom2 = st.columns(2)
        
        with kolom1:
            kunci_utama = st.selectbox("🔑 Pilih Kolom Acuan di File Utama:", df_bersih.columns)
        with kolom2:
            kunci_ref = st.selectbox("🔑 Pilih Kolom Acuan di File Referensi:", df_ref.columns)
            
        # Tombol Eksekusi VLOOKUP
        if st.button("🚀 Jalankan VLOOKUP Sekarang!"):
            # PROSES MERGE (VLOOKUP ala Python)
            # how='left' memastikan data utama tidak ada yang hilang
            df_hasil = pd.merge(df_bersih, df_ref, left_on=kunci_utama, right_on=kunci_ref, how='left')
            
            st.success("✅ VLOOKUP Berhasil! Ini adalah hasil gabungannya:")
            st.dataframe(df_hasil)
            
            # FASE UNDUH HASIL VLOOKUP
            st.markdown("---")
            st.subheader("📥 Unduh Hasil Akhir")
            jumlah_baris = len(df_hasil)
            
            if jumlah_baris > 50000:
                st.warning(f"⚠️ Data raksasa ({jumlah_baris} baris). Hanya bisa unduh CSV.")
                csv_data = df_hasil.to_csv(index=False).encode('utf-8')
                st.download_button("📄 Download Data Bersih (.csv)", data=csv_data, file_name="Hasil_VLOOKUP.csv", mime="text/csv")
            else:
                col_dl1, col_dl2 = st.columns(2)
                with col_dl1:
                    buffer_excel = io.BytesIO()
                    df_hasil.to_excel(buffer_excel, index=False, engine='openpyxl')
                    st.download_button("📊 Download Hasil (.xlsx)", data=buffer_excel.getvalue(), file_name="Hasil_VLOOKUP.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                with col_dl2:
                    csv_data = df_hasil.to_csv(index=False).encode('utf-8')
                    st.download_button("📄 Download Hasil (.csv)", data=csv_data, file_name="Hasil_VLOOKUP.csv", mime="text/csv")

    else:
        st.info("💡 Jika ingin melakukan VLOOKUP, silakan unggah **File Referensi** di menu sebelah kiri.")
        
else:
    st.info("👈 Silakan unggah **File Utama** di menu sebelah kiri untuk memulai.")
