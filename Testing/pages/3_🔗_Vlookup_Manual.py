import streamlit as st
import pandas as pd
import io
import numpy as np
from openpyxl.styles import PatternFill

st.set_page_config(page_title="Ultimate Data Pipeline", layout="wide")
st.title("⚙️ Hub Otomatisasi Data Universal (Ultimate Edition)")
st.write("Bangun alur pemrosesan data Anda sendiri. Fleksibilitas penuh dengan kekuatan Cascading ETL dan N-Tier Fallback.")

# ==========================================
# 0. INISIALISASI MEMORI (BRANKAS)
# ==========================================
def init_state(key, default):
    if key not in st.session_state:
        st.session_state[key] = default

init_state('id_ganti', [])
init_state('id_fb', [])
init_state('fb_conds', {}) 
init_state('id_hapus', [])
init_state('id_warna', [])
init_state('id_split2', [1]) 
init_state('id_split3', [1]) 
init_state('proses_selesai', False)
init_state('hasil_tabel', {})

def tambah_item(list_name): 
    st.session_state[list_name].append(np.random.randint(10000, 99999))
def hapus_item(list_name, uid): 
    st.session_state[list_name].remove(uid)

def tambah_fb_layer():
    new_id = np.random.randint(10000, 99999)
    st.session_state.id_fb.append(new_id)
    st.session_state.fb_conds[new_id] = [np.random.randint(10000, 99999)]

def hapus_fb_layer(uid):
    st.session_state.id_fb.remove(uid)
    if uid in st.session_state.fb_conds:
        del st.session_state.fb_conds[uid]

def tambah_fb_cond(layer_uid):
    if layer_uid not in st.session_state.fb_conds:
        st.session_state.fb_conds[layer_uid] = []
    st.session_state.fb_conds[layer_uid].append(np.random.randint(10000, 99999))

def hapus_fb_cond(layer_uid, cond_uid):
    st.session_state.fb_conds[layer_uid].remove(cond_uid)

# ==========================================
# 1. PINTU MASUK FILE (3 KOLOM)
# ==========================================
st.markdown("### 📥 Input File Data")
col1, col2, col3 = st.columns(3)
with col1:
    file_u = st.file_uploader("1. Tabel Utama [Wajib]", type=["xlsx", "csv"])
    head_u = st.number_input("Header Utama:", 0, step=1) if file_u else 0
with col2:
    file_k = st.file_uploader("2. Tabel Kamus Utama [Wajib]", type=["xlsx", "csv"])
    head_k = st.number_input("Header Kamus Utama:", 0, step=1) if file_k else 0
with col3:
    file_o = st.file_uploader("3. Kamus Opsional (Kemarin) [Opsional]", type=["xlsx", "csv"])
    head_o = st.number_input("Header Kamus Opsional:", 0, step=1) if file_o else 0

if file_u and file_k:
    try:
        def baca(f, h):
            if not f: return pd.DataFrame()
            if f.name.endswith('.csv'): return pd.read_csv(f, header=h, dtype=str)
            return pd.read_excel(pd.ExcelFile(f), header=h, dtype=str)

        df_u = baca(file_u, head_u)
        df_k = baca(file_k, head_k)
        df_o = baca(file_o, head_o) if file_o else pd.DataFrame()
        
        def rapi_tgl(df):
            if df.empty: return df
            kr = []
            for col in df.columns:
                cs = str(col)
                if ' 00:00:00' in cs:
                    try: kr.append(pd.to_datetime(cs).strftime('%d-%b'))
                    except: kr.append(cs)
                else: kr.append(cs)
            df.columns = kr
            return df
            
        df_u = rapi_tgl(df_u)
        df_k = rapi_tgl(df_k)
        df_o = rapi_tgl(df_o)
        
        kolom_dinamis = list(df_u.columns)
        dict_kamus = {"Kamus Utama": df_k}
        if not df_o.empty: dict_kamus["Kamus Opsional"] = df_o
        pilihan_kamus = list(dict_kamus.keys())
        
    except Exception as e:
        st.error(f"Gagal membaca file: {e}")
        st.stop()

    st.markdown("---")

    # ==========================================
    # UI TAHAP 1: PEMBERSIHAN AWAL (FIND & REPLACE)
    # ==========================================
    st.subheader("🧹 Tahap 1: Pembersihan & Ganti Data")
    
    daftar_kolom_utama = ["(Lewati)"] + kolom_dinamis
    pilihan_hapus_kosong = st.selectbox("Hapus baris jika kolom ini kosong (Biasanya Baris Total):", daftar_kolom_utama)
    st.markdown("---")
    
    st.markdown("**Cari dan Ganti Nilai (Find & Replace Sebelum VLOOKUP):**")
    for uid in st.session_state.id_ganti:
        c1, c2, c3, c4, c5 = st.columns([2, 4, 2, 2, 1])
        with c1: kol_g = st.selectbox("Di Kolom:", kolom_dinamis, key=f"kg_{uid}")
        with c2: 
            nilai_unik = df_u[kol_g].dropna().astype(str).unique().tolist() if kol_g in df_u.columns else []
            cari_g = st.multiselect("Pilih data yang diganti:", nilai_unik, key=f"cg_{uid}")
        with c3: tipe_g = st.selectbox("Ubah menjadi:", ["Teks Baru", "Kosong (NaN/Blank)"], key=f"tg_{uid}")
        with c4: ganti_g = st.text_input("Teks Baru:", key=f"gg_{uid}") if tipe_g == "Teks Baru" else ""
        with c5: 
            st.markdown("<br>", unsafe_allow_html=True)
            st.button("❌", key=f"dx_{uid}", on_click=hapus_item, args=('id_ganti', uid))
            
    st.button("➕ Tambah Aturan Ganti Data", key="btn_add_ganti", on_click=tambah_item, args=('id_ganti',))
    st.markdown("---")

    # ==========================================
    # UI TAHAP 1.5: VLOOKUP AWAL (DARI KAMUS OPSIONAL)
    # ==========================================
    st.subheader("🕰️ Tahap 1.5: VLOOKUP Awal (Dari Kamus Opsional)")
    tarik_awal_aktif = False
    
    if not df_o.empty:
        tarik_awal_aktif = st.checkbox("Aktifkan Tarik Data Awal (Contoh: Tarik RMCode Kemarin)", value=True)
        if tarik_awal_aktif:
            c1, c2, c3 = st.columns(3)
            with c1: kunci_u_awal = st.selectbox("Kunci di Tabel Utama:", kolom_dinamis, key="ku_awal")
            with c2: kunci_o_awal = st.selectbox("Kunci di Kamus Opsional:", df_o.columns, key="ko_awal")
            with c3: target_o_awal = st.selectbox("Data yang Ditarik:", df_o.columns, key="to_awal")
            
            c4, c5 = st.columns(2)
            with c4: mode_awal = st.radio("Penempatan:", ["Buat Kolom Baru", "Timpa Kolom Lama"], key="ma_awal", horizontal=True)
            with c5:
                if mode_awal == "Buat Kolom Baru":
                    nama_target_awal = st.text_input("Nama Kolom Baru:", value=target_o_awal, key="nt_awal")
                    if nama_target_awal and nama_target_awal not in kolom_dinamis:
                        kolom_dinamis.append(nama_target_awal)
                else:
                    nama_target_awal = st.selectbox("Pilih Kolom Ditimpa:", kolom_dinamis, key="nt_awal_timpa")
                    
            st.markdown("**Pemisahan Tabel Pertama:**")
            pisah_awal_aktif = st.checkbox("Pisahkan baris yang berhasil ditarik ke file terpisah (Match Kemarin)", value=True)
            if pisah_awal_aktif:
                nama_t1 = st.text_input("Nama File Output Tahap 1:", value="Tabel 1 (Match Opsional)", key="nama_t1")
    else:
        st.info("Unggah Kamus Opsional (File 3) untuk membuka pengaturan ini.")
    st.markdown("---")

    # ==========================================
    # UI TAHAP 2: VLOOKUP UTAMA, VALIDASI & SPLIT 2
    # ==========================================
    st.subheader("🔗 Tahap 2: VLOOKUP Utama, Validasi Silang, & Ekstraksi")
    t2_aktif = st.checkbox("Aktifkan VLOOKUP Utama", value=True)
    if t2_aktif:
        st.markdown("**1. Tarik Data dari Kamus Utama**")
        c1, c2 = st.columns(2)
        with c1: t2_ku = st.selectbox("Kunci Utama:", kolom_dinamis, key="t2_ku")
        with c2: t2_kk = st.selectbox("Kunci Kamus Utama:", df_k.columns, key="t2_kk")
        
        t2_tarikan = st.multiselect("Pilih Data yang Ditarik:", df_k.columns, key="t2_tarik")
        t2_map_kolom = {}
        for t in t2_tarikan:
            c_m1, c_m2 = st.columns(2)
            with c_m1:
                mode_t2 = st.radio(f"Penempatan untuk '{t}':", ["Buat Kolom Baru", "Timpa Kolom Lama"], key=f"mode_t2_{t}", horizontal=True)
            with c_m2:
                if mode_t2 == "Buat Kolom Baru":
                    nama_t2_t = st.text_input(f"Nama Kolom Baru:", value=t, key=f"t2_n_{t}")
                    if nama_t2_t and nama_t2_t not in kolom_dinamis: kolom_dinamis.append(nama_t2_t)
                    t2_map_kolom[t] = {"mode": mode_t2, "target": nama_t2_t}
                else:
                    nama_t2_t = st.selectbox(f"Pilih Kolom Ditimpa:", kolom_dinamis, key=f"t2_s_{t}")
                    t2_map_kolom[t] = {"mode": mode_t2, "target": nama_t2_t}

        st.markdown("**2. Validasi Silang (Pembuatan Parameter <<cek>>)**")
        val_aktif = st.checkbox("Aktifkan Validasi Silang")
        if val_aktif:
            c1, c2, c3 = st.columns(3)
            with c1: val_kiri = st.selectbox("Cek Kolom Kiri:", kolom_dinamis, key="vk_kiri")
            with c2: val_op = st.selectbox("Kondisi:", ["Sama Dengan (==)", "Tidak Sama (!=)"], key="vk_op")
            with c3: val_kanan = st.selectbox("Dengan Kolom Kanan:", kolom_dinamis, key="vk_kanan")
            
            c4, c5, c6 = st.columns(3)
            with c4: 
                val_hasil = st.text_input("Simpan Hasil di Kolom:", value="<<cek>>", key="vk_hasil")
                if val_hasil not in kolom_dinamis: kolom_dinamis.append(val_hasil)
            with c5: val_b = st.text_input("Jika Benar isi:", value="0", key="vk_b")
            with c6: val_s = st.text_input("Jika Salah isi:", value="1", key="vk_s")

        st.markdown("**3. Ekstraksi Tabel 2 (Syarat Tak Terbatas & Aksi Terarah):**")
        pisah_t2_aktif = st.checkbox("Aktifkan Pemisahan Tabel 2", value=True)
        if pisah_t2_aktif:
            c1, c2 = st.columns([2, 3])
            with c1: nama_t2 = st.text_input("Nama File Output Tahap 2:", value="Tabel 2 (Valid)", key="nama_t2")
            with c2: 
                aksi_t2 = st.radio("Tindakan untuk baris yang MEMENUHI syarat di bawah:", 
                    ["Dilempar ke Tahap 3 (Sisa data yang bersih disimpan di Tabel 2)", 
                     "Simpan di Tabel 2 (Sisa data dilempar ke Tahap 3)"], 
                    key="aksi_t2")
                
            logika_t2 = st.radio("Logika Penggabungan Syarat:", ["Wajib Penuhi SEMUA Syarat (AND)", "Penuhi SALAH SATU Syarat (OR)"], horizontal=True, key="logika_t2")
            
            for uid in st.session_state.id_split2:
                c1, c2, c3, c4 = st.columns([3, 2, 3, 1])
                with c1: k_val = st.selectbox("Kolom:", kolom_dinamis, key=f"s2k_{uid}")
                with c2: o_val = st.selectbox("Kondisi:", ["Sama Dengan", "Tidak Sama", "Mengandung", "TIDAK Mengandung", "Kosong", "Tidak Kosong"], key=f"s2o_{uid}")
                with c3: 
                    if o_val in ["Sama Dengan", "Tidak Sama"]:
                        if k_val in df_u.columns:
                            ops_u = ["(Ketik Manual)"] + list(df_u[k_val].dropna().astype(str).unique())
                            pil_v = st.selectbox("Pilih Nilai:", ops_u, key=f"s2v_sel_{uid}")
                            if pil_v == "(Ketik Manual)":
                                st.text_input("Ketik Manual:", key=f"s2v_man_{uid}")
                        else:
                            st.text_input("Ketik Nilai (Kolom baru dibuat mesin):", key=f"s2v_man_{uid}")
                    elif o_val not in ["Kosong", "Tidak Kosong"]:
                        st.text_input("Nilai Teks:", key=f"s2v_{uid}")
                with c4: 
                    st.markdown("<br>", unsafe_allow_html=True)
                    st.button("❌", key=f"s2x_{uid}", on_click=hapus_item, args=('id_split2', uid))
            st.button("➕ Tambah Syarat (Tahap 2)", key="btn_add_split2", on_click=tambah_item, args=('id_split2',))
    st.markdown("---")

    # ==========================================
    # UI TAHAP 3: FALLBACK & PEMISAHAN AKHIR
    # ==========================================
    st.subheader("⚠️ Tahap 3: Mesin N-Tier Fallback & Pemisahan Akhir")
    t3_aktif = st.checkbox("Aktifkan Tahap 3 (Proses Sisa Data Anomali)", value=True)
    if t3_aktif:
        st.markdown("**1. Mesin N-Tier Fallback (Eksekusi VLOOKUP cadangan pada sisa data SEBELUM dipisah):**")
        target_fb = st.selectbox("Kolom Target yang akan diisi Fallback:", kolom_dinamis, key="tfb_target")
        
        reset_target_fb = st.checkbox("Kosongkan (Reset) isi Kolom Target di atas sebelum Fallback pertama berjalan agar data lama (seperti 'SPH') tertimpa.", value=True)

        for urutan, uid in enumerate(st.session_state.id_fb):
            if uid not in st.session_state.fb_conds: st.session_state.fb_conds[uid] = []
            
            with st.container(border=True):
                st.markdown(f"**Lapisan Fallback {urutan+1}**")
                c1, c2, c3 = st.columns([1,1,1])
                with c1: fb_kamus = st.selectbox("Gunakan Kamus:", pilihan_kamus, key=f"fbk_{uid}")
                with c2: fb_ku = st.selectbox("Kunci Utama:", kolom_dinamis, key=f"fbku_{uid}")
                with c3: fb_kk = st.selectbox("Kunci Kamus Utama:", dict_kamus[fb_kamus].columns, key=f"fbkk_{uid}")
                
                st.markdown("*Saringan Kamus Khusus Lapisan Ini:*")
                logika_fb = st.radio("Logika Gabungan Filter:", ["Wajib Penuhi SEMUA (AND)", "Penuhi SALAH SATU (OR)"], key=f"fblog_{uid}", horizontal=True)
                
                for c_uid in st.session_state.fb_conds[uid]:
                    c4, c5, c6, c7 = st.columns([3, 2, 3, 1])
                    with c4: f_sk = st.selectbox("Filter Kolom Kamus:", ["(Tanpa Filter)"] + list(dict_kamus[fb_kamus].columns), key=f"fbsk_{uid}_{c_uid}")
                    with c5: f_op = st.selectbox("Kondisi:", ["Sama Dengan", "Tidak Sama", "Mengandung", "TIDAK Mengandung", "Kosong", "Tidak Kosong"], key=f"fbso_{uid}_{c_uid}")
                    with c6: f_val = st.text_input("Nilai Teks:", key=f"fbsv_{uid}_{c_uid}")
                    with c7: 
                        st.markdown("<br>", unsafe_allow_html=True)
                        st.button("❌", key=f"fbx_c_{uid}_{c_uid}", on_click=hapus_fb_cond, args=(uid, c_uid))
                        
                c_add, c_del = st.columns([2, 8])
                with c_add: st.button("➕ Tambah Syarat Filter", key=f"add_c_{uid}", on_click=tambah_fb_cond, args=(uid,))
                with c_del: st.button("🗑️ Hapus Lapisan Fallback Ini", key=f"fbx_{uid}", on_click=hapus_fb_layer, args=(uid,))
                
        st.button("➕ Tambah Lapisan Fallback Baru", key="btn_add_fb_layer", on_click=tambah_fb_layer)

        st.markdown("**2. Pemisahan Tabel Akhir (Membelah sisa data menjadi 2 File):**")
        pisah_t3_aktif = st.checkbox("Aktifkan Pemisahan Tabel 3", value=True)
        if pisah_t3_aktif:
            c1, c2 = st.columns(2)
            with c1: nama_t3a = st.text_input("Nama File (Ekstraksi):", value="Tabel 3A (Ekstraksi)", key="nama_t3a")
            with c2: nama_t3b = st.text_input("Nama File (Sisa Akhir):", value="Tabel 3B (Sisa Akhir)", key="nama_t3b")
            
            aksi_t3 = st.radio("Baris yang MEMENUHI syarat di bawah akan dimasukkan ke:", ["Tabel 3A", "Tabel 3B"], horizontal=True, key="aksi_t3")
            logika_t3 = st.radio("Logika Penggabungan Syarat (Tahap 3):", ["Wajib Penuhi SEMUA Syarat (AND)", "Penuhi SALAH SATU Syarat (OR)"], horizontal=True, key="logika_t3")
            
            for uid in st.session_state.id_split3:
                c1, c2, c3, c4 = st.columns([3, 2, 3, 1])
                with c1: k_val = st.selectbox("Kolom:", kolom_dinamis, key=f"s3k_{uid}")
                with c2: o_val = st.selectbox("Kondisi:", ["Sama Dengan", "Tidak Sama", "Mengandung", "TIDAK Mengandung", "Kosong", "Tidak Kosong"], key=f"s3o_{uid}")
                with c3: 
                    if o_val in ["Sama Dengan", "Tidak Sama"]:
                        if k_val in df_u.columns:
                            ops_u = ["(Ketik Manual)"] + list(df_u[k_val].dropna().astype(str).unique())
                            pil_v = st.selectbox("Pilih Nilai:", ops_u, key=f"s3v_sel_{uid}")
                            if pil_v == "(Ketik Manual)":
                                st.text_input("Ketik Manual:", key=f"s3v_man_{uid}")
                        else:
                            st.text_input("Ketik Nilai (Kolom Baru):", key=f"s3v_man_{uid}")
                    elif o_val not in ["Kosong", "Tidak Kosong"]:
                        st.text_input("Nilai Teks:", key=f"s3v_{uid}")
                with c4: 
                    st.markdown("<br>", unsafe_allow_html=True)
                    st.button("❌", key=f"s3x_{uid}", on_click=hapus_item, args=('id_split3', uid))
            st.button("➕ Tambah Syarat (Tahap 3)", key="btn_add_split3", on_click=tambah_item, args=('id_split3',))
    st.markdown("---")

    # ==========================================
    # UI TAHAP 4: MODIFIKASI SPESIFIK (ERASER)
    # ==========================================
    st.subheader("🗑️ Tahap 4: Modifikasi Spesifik (Eraser Target)")
    st.write("Kosongkan nilai kolom pada hasil tabel tertentu SETELAH semua proses di atas selesai.")
    
    opsi_tabel_target = ["Semua Tabel"]
    if 'nama_t1' in st.session_state: opsi_tabel_target.append(st.session_state.nama_t1)
    if 'nama_t2' in st.session_state: opsi_tabel_target.append(st.session_state.nama_t2)
    if 'nama_t3a' in st.session_state: opsi_tabel_target.append(st.session_state.nama_t3a)
    if 'nama_t3b' in st.session_state: opsi_tabel_target.append(st.session_state.nama_t3b)

    for uid in st.session_state.id_hapus:
        c1, c2, c3, c4 = st.columns([2,2,2,1])
        with c1: hapus_lokasi = st.selectbox("Target Tabel:", opsi_tabel_target, key=f"hl_{uid}")
        with c2: hapus_target = st.selectbox("Kosongkan Kolom:", kolom_dinamis, key=f"ht_{uid}")
        with c3: hapus_syarat = st.text_input("JIKA sel berisi teks (Kosongkan jika hapus semua):", key=f"hs_{uid}")
        with c4: 
            st.markdown("<br>", unsafe_allow_html=True)
            st.button("❌", key=f"hx_{uid}", on_click=hapus_item, args=('id_hapus', uid))
    st.button("➕ Tambah Aturan Eraser", key="btn_add_eraser", on_click=tambah_item, args=('id_hapus',))
    st.markdown("---")

    # ==========================================
    # UI TAHAP 5: KOSMETIK & WARNA
    # ==========================================
    st.subheader("🎨 Tahap 5: Kosmetik & Tata Letak Akhir")
    with st.expander("Buka Pengaturan Kosmetik"):
        c1, c2 = st.columns(2)
        with c1: 
            sort_kol = st.multiselect("Urutkan Data (Sorting) Berdasarkan:", kolom_dinamis)
            sort_asc = st.radio("Arah Urutan:", ["A-Z", "Z-A"]) == "A-Z"
        with c2:
            reindex_patokan = st.selectbox("Pindahkan semua kolom baru tepat ke sebelah KANAN dari:", ["(Biarkan di ujung kanan)"] + list(df_u.columns))
            teks_mutlak = st.multiselect("Pertahankan format Teks (Mencegah Excel menghilangkan angka 0):", kolom_dinamis)

        st.markdown("**Pengecatan Warna Dinamis Excel:**")
        for uid in st.session_state.id_warna:
            c1, c2, c3, c4 = st.columns([3,2,2,1])
            with c1: w_kol = st.multiselect("Pilih Kolom:", kolom_dinamis, key=f"wk_{uid}")
            with c2: w_jud = st.color_picker("Warna Judul:", "#548235", key=f"wj_{uid}")
            with c3: w_isi = st.color_picker("Warna Sel:", "#E2EFDA", key=f"ws_{uid}")
            with c4: 
                st.markdown("<br>", unsafe_allow_html=True)
                st.button("❌", key=f"wx_{uid}", on_click=hapus_item, args=('id_warna', uid))
        st.button("➕ Tambah Aturan Warna", key="btn_add_warna", on_click=tambah_item, args=('id_warna',))

    # ==========================================
    # MESIN EKSEKUSI PIPELINE
    # ==========================================
    st.markdown("---")
    if st.button("🚀 EKSEKUSI SELURUH PIPELINE!", use_container_width=True):
        with st.spinner("Mesin Pipeline sedang merakit tabel..."):
            try:
                df_run = df_u.copy()
                st.session_state.hasil_tabel = {}

                def clean_str(s_col): 
                    return s_col.fillna('').astype(str).str.strip().str.upper()

                def build_mask(df, kol, op, val):
                    if kol not in df.columns: return pd.Series(False, index=df.index)
                    k_str = clean_str(df[kol])
                    v_str = str(val).strip().upper()
                    if op == "Kosong": return df[kol].isna() | (k_str == '') | (k_str == 'NAN')
                    if op == "Tidak Kosong": return df[kol].notna() & (k_str != '') & (k_str != 'NAN')
                    if op == "Sama Dengan": return k_str == v_str
                    if op == "Tidak Sama": return k_str != v_str
                    if op == "Mengandung": return k_str.str.contains(v_str, case=False, regex=False)
                    if op == "TIDAK Mengandung": return ~k_str.str.contains(v_str, case=False, regex=False)
                    return pd.Series(False, index=df.index)

                # 0. HAPUS BARIS KOSONG TOTAL
                if pilihan_hapus_kosong != "(Lewati)":
                    df_run[pilihan_hapus_kosong] = df_run[pilihan_hapus_kosong].fillna('').astype(str).str.strip()
                    df_run = df_run[df_run[pilihan_hapus_kosong] != '']

                # 1. FIND & REPLACE
                for uid in st.session_state.id_ganti:
                    kg = st.session_state[f"kg_{uid}"]
                    cg = st.session_state.get(f"cg_{uid}", [])
                    tg = st.session_state.get(f"tg_{uid}", "Teks Baru")
                    if kg in df_run.columns and len(cg) > 0:
                        df_run[kg] = df_run[kg].astype(str)
                        if tg == "Kosong (NaN/Blank)": df_run[kg] = df_run[kg].replace(cg, np.nan)
                        else: df_run[kg] = df_run[kg].replace(cg, st.session_state.get(f"gg_{uid}", ""))

                # 1.5. VLOOKUP AWAL & SPLIT 1
                if not df_o.empty and tarik_awal_aktif:
                    df_run[kunci_u_awal] = clean_str(df_run[kunci_u_awal])
                    df_o[kunci_o_awal] = clean_str(df_o[kunci_o_awal])
                    dict_awal = df_o.drop_duplicates(subset=[kunci_o_awal]).set_index(kunci_o_awal)[target_o_awal].to_dict()
                    
                    if mode_awal == "Buat Kolom Baru": df_run[nama_target_awal] = df_run[kunci_u_awal].map(dict_awal)
                    else: df_run[nama_target_awal] = df_run[kunci_u_awal].map(dict_awal).fillna(df_run[nama_target_awal])
                        
                    if pisah_awal_aktif and nama_target_awal in df_run.columns:
                        mask_t1 = build_mask(df_run, nama_target_awal, "Tidak Kosong", "")
                        st.session_state.hasil_tabel[nama_t1] = df_run[mask_t1].copy()
                        df_run = df_run[~mask_t1].copy()

                # 2. VLOOKUP UTAMA, VALIDASI & SPLIT 2
                if not df_run.empty and t2_aktif:
                    df_run[t2_ku] = clean_str(df_run[t2_ku])
                    df_k_t2 = df_k.copy()
                    df_k_t2[t2_kk] = clean_str(df_k_t2[t2_kk])
                    
                    for t, conf in t2_map_kolom.items():
                        d_k = df_k_t2.drop_duplicates(subset=[t2_kk]).set_index(t2_kk)[t].to_dict()
                        if conf['mode'] == "Buat Kolom Baru":
                            df_run[conf['target']] = df_run[t2_ku].map(d_k)
                        else:
                            df_run[conf['target']] = df_run[t2_ku].map(d_k).fillna(df_run[conf['target']])
                        
                    if val_aktif:
                        vk, vn = clean_str(df_run[val_kiri]), clean_str(df_run[val_kanan])
                        kondisi_v = (vk == vn) if val_op == "Sama Dengan (==)" else (vk != vn)
                        df_run[val_hasil] = val_s
                        df_run.loc[kondisi_v, val_hasil] = val_b

                    if pisah_t2_aktif:
                        if len(st.session_state.id_split2) > 0:
                            is_and = (logika_t2 == "Wajib Penuhi SEMUA Syarat (AND)")
                            mask_t2 = pd.Series(True, index=df_run.index) if is_and else pd.Series(False, index=df_run.index)
                            
                            for uid in st.session_state.id_split2:
                                k = st.session_state.get(f"s2k_{uid}", "")
                                o = st.session_state.get(f"s2o_{uid}", "")
                                
                                if o in ["Sama Dengan", "Tidak Sama"]:
                                    if k in df_u.columns:
                                        sel_v = st.session_state.get(f"s2v_sel_{uid}", "")
                                        v = st.session_state.get(f"s2v_man_{uid}", "") if sel_v == "(Ketik Manual)" else sel_v
                                    else:
                                        v = st.session_state.get(f"s2v_man_{uid}", "")
                                elif o in ["Kosong", "Tidak Kosong"]:
                                    v = ""
                                else:
                                    v = st.session_state.get(f"s2v_{uid}", "")
                                    
                                m_temp = build_mask(df_run, k, o, v)
                                mask_t2 = (mask_t2 & m_temp) if is_and else (mask_t2 | m_temp)
                        else:
                            mask_t2 = pd.Series(True, index=df_run.index)
                            
                        aksi_t2 = st.session_state.get("aksi_t2", "")
                        if "Dilempar ke Tahap 3" in aksi_t2:
                            st.session_state.hasil_tabel[nama_t2] = df_run[~mask_t2].copy() 
                            df_run = df_run[mask_t2].copy() 
                        else:
                            st.session_state.hasil_tabel[nama_t2] = df_run[mask_t2].copy()
                            df_run = df_run[~mask_t2].copy()

                # 3. FALLBACK & SPLIT 3
                if not df_run.empty and t3_aktif:
                    if target_fb in df_run.columns:
                        
                        if reset_target_fb:
                            df_run[target_fb] = np.nan
                        
                        for uid in st.session_state.id_fb:
                            f_kam = st.session_state[f"fbk_{uid}"]
                            f_ku = st.session_state[f"fbku_{uid}"]
                            f_kk = st.session_state[f"fbkk_{uid}"]
                            
                            df_run[f_ku] = clean_str(df_run[f_ku])
                            df_f = dict_kamus[f_kam].copy() if f_kam in dict_kamus else df_k.copy()
                            df_f[f_kk] = clean_str(df_f[f_kk])
                            
                            cond_list = st.session_state.fb_conds.get(uid, [])
                            if len(cond_list) > 0:
                                is_and_fb = (st.session_state.get(f"fblog_{uid}", "") == "Wajib Penuhi SEMUA (AND)")
                                mask_fb = pd.Series(True, index=df_f.index) if is_and_fb else pd.Series(False, index=df_f.index)
                                filter_aktif = False
                                
                                for c_uid in cond_list:
                                    f_sk = st.session_state.get(f"fbsk_{uid}_{c_uid}", "(Tanpa Filter)")
                                    if f_sk != "(Tanpa Filter)":
                                        filter_aktif = True
                                        f_op = st.session_state.get(f"fbso_{uid}_{c_uid}", "")
                                        f_val = st.session_state.get(f"fbsv_{uid}_{c_uid}", "")
                                        m_temp = build_mask(df_f, f_sk, f_op, f_val)
                                        mask_fb = (mask_fb & m_temp) if is_and_fb else (mask_fb | m_temp)
                                        
                                if filter_aktif:
                                    df_f = df_f[mask_fb]
                            
                            col_tarik_asli = t2_tarikan[0] if (t2_aktif and len(t2_tarikan)>0) else target_fb
                            try: d_dict = df_f.drop_duplicates(subset=[f_kk]).set_index(f_kk)[col_tarik_asli].to_dict()
                            except: d_dict = {}
                            
                            mk = build_mask(df_run, target_fb, "Kosong", "")
                            df_run.loc[mk, target_fb] = df_run.loc[mk, f_ku].map(d_dict)

                    if pisah_t3_aktif:
                        if len(st.session_state.id_split3) > 0:
                            is_and = (logika_t3 == "Wajib Penuhi SEMUA Syarat (AND)")
                            mask_t3 = pd.Series(True, index=df_run.index) if is_and else pd.Series(False, index=df_run.index)
                            
                            for uid in st.session_state.id_split3:
                                k = st.session_state.get(f"s3k_{uid}", "")
                                o = st.session_state.get(f"s3o_{uid}", "")
                                
                                if o in ["Sama Dengan", "Tidak Sama"]:
                                    if k in df_u.columns:
                                        sel_v = st.session_state.get(f"s3v_sel_{uid}", "")
                                        v = st.session_state.get(f"s3v_man_{uid}", "") if sel_v == "(Ketik Manual)" else sel_v
                                    else:
                                        v = st.session_state.get(f"s3v_man_{uid}", "")
                                elif o in ["Kosong", "Tidak Kosong"]:
                                    v = ""
                                else:
                                    v = st.session_state.get(f"s3v_{uid}", "")
                                    
                                m_temp = build_mask(df_run, k, o, v)
                                mask_t3 = (mask_t3 & m_temp) if is_and else (mask_t3 | m_temp)
                        else:
                            mask_t3 = pd.Series(True, index=df_run.index)
                            
                        aksi_t3 = st.session_state.get("aksi_t3", "Tabel 3A")
                        if aksi_t3 == "Tabel 3A":
                            st.session_state.hasil_tabel[nama_t3a] = df_run[mask_t3].copy()
                            st.session_state.hasil_tabel[nama_t3b] = df_run[~mask_t3].copy()
                        else:
                            st.session_state.hasil_tabel[nama_t3a] = df_run[~mask_t3].copy()
                            st.session_state.hasil_tabel[nama_t3b] = df_run[mask_t3].copy()
                        df_run = pd.DataFrame() 
                    else:
                        st.session_state.hasil_tabel["Data Akhir"] = df_run
                        df_run = pd.DataFrame()

                # 4. MODIFIKASI SPESIFIK / ERASER (SETELAH SEMUA TABEL TERBENTUK)
                for uid in st.session_state.id_hapus:
                    h_lokasi = st.session_state[f"hl_{uid}"]
                    h_targ = st.session_state[f"ht_{uid}"]
                    h_syar = st.session_state[f"hs_{uid}"].strip().upper()
                    
                    target_dfs = []
                    if h_lokasi == "Semua Tabel": target_dfs = list(st.session_state.hasil_tabel.keys())
                    elif h_lokasi in st.session_state.hasil_tabel: target_dfs = [h_lokasi]
                    
                    for t_name in target_dfs:
                        df_target = st.session_state.hasil_tabel[t_name]
                        if h_targ in df_target.columns:
                            if h_syar == "": 
                                df_target[h_targ] = np.nan
                            else: 
                                df_target.loc[clean_str(df_target[h_targ]).str.contains(h_syar, case=False, regex=False), h_targ] = np.nan
                            st.session_state.hasil_tabel[t_name] = df_target

                # 5. KOSMETIK (Re-index, Tipe Data, Sort)
                for nama_tab, df_res in st.session_state.hasil_tabel.items():
                    if not df_res.empty:
                        if reindex_patokan != "(Biarkan di ujung kanan)":
                            cols = list(df_u.columns)
                            new_cols = [c for c in df_res.columns if c not in cols]
                            if reindex_patokan in cols:
                                idx_p = cols.index(reindex_patokan) + 1
                                for nc in new_cols:
                                    cols.insert(idx_p, nc)
                                    idx_p += 1
                            df_res = df_res[[c for c in cols if c in df_res.columns]]

                        for c in df_res.columns:
                            if c not in teks_mutlak: df_res[c] = pd.to_numeric(df_res[c], errors='ignore')

                        if sort_kol:
                            valid_sort = [k for k in sort_kol if k in df_res.columns]
                            if valid_sort: df_res.sort_values(by=valid_sort, ascending=sort_asc, inplace=True, na_position='last')
                            
                        st.session_state.hasil_tabel[nama_tab] = df_res

                st.session_state.proses_selesai = True

            except Exception as e:
                import traceback
                st.error(f"❌ Terjadi kesalahan mesin: {e}. Detail: {traceback.format_exc()}")

    # ==========================================
    # UNDUH HASIL
    # ==========================================
    if st.session_state.proses_selesai:
        st.success("✅ Alur Pipa Data Selesai Dieksekusi!")
        st.markdown("---")
        
        def buat_excel(df):
            b = io.BytesIO()
            with pd.ExcelWriter(b, engine='openpyxl') as w: 
                df.to_excel(w, index=False)
                ws = w.sheets['Sheet1']
                for uid in st.session_state.id_warna:
                    k_warn = st.session_state.get(f"wk_{uid}", [])
                    if k_warn:
                        cj = PatternFill(start_color=st.session_state[f"wj_{uid}"].lstrip('#'), end_color=st.session_state[f"wj_{uid}"].lstrip('#'), fill_type="solid")
                        ci = PatternFill(start_color=st.session_state[f"ws_{uid}"].lstrip('#'), end_color=st.session_state[f"ws_{uid}"].lstrip('#'), fill_type="solid")
                        for kw in k_warn:
                            if kw in df.columns:
                                idx_w = df.columns.get_loc(kw) + 1
                                ws.cell(row=1, column=idx_w).fill = cj
                                for r in range(2, len(df) + 2): ws.cell(row=r, column=idx_w).fill = ci
            return b.getvalue()
            
        cols_dl = st.columns(len(st.session_state.hasil_tabel))
        for idx, (nama_file, data_tabel) in enumerate(st.session_state.hasil_tabel.items()):
            with cols_dl[idx]:
                st.info(f"📁 **{nama_file}**\nBerisi {len(data_tabel)} baris")
                st.download_button(label=f"⬇️ Unduh File", data=buat_excel(data_tabel), file_name=f"{nama_file}.xlsx", key=f"dl_{idx}")
else:
    st.info("👈 Silakan unggah minimal Tabel Utama dan Kamus Utama untuk memunculkan panel pengaturan.")
