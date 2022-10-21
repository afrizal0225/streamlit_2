from io import BytesIO
import streamlit as st
import pandas as pd
import numpy as np

st.title('TRANSFORM DATA PLATFORM')

upload_file1 = st.file_uploader('choose file to transform')
upload_file2 = st.file_uploader('choose data validation')

data1 = pd.read_excel(upload_file1)
data2 = pd.read_excel(upload_file2, sheet_name='Sheet3')


def cheking_2(df,df_validasi_stock_ideal,df_validasi_artikel_ideal):
    # clean data format
    df = df.rename(columns={df.columns[0]: "Drop1",
                            df.columns[1]: "Artikel ID",
                            df.columns[2]: "Drop2",
                            df.columns[3]: "Drop3",
                            df.columns[4]: "Nama Artikel",
                            df.columns[5]: "Drop4",
                            df.columns[6]: "Drop5",
                            df.columns[7]: "Umur",
                            df.columns[8]: "Last Action",
                            df.columns[9]: "Last Action Date",
                            df.columns[10]: "Penjualan 3 Bulan Sebelum",
                            df.columns[11]: "Penjualan 2 Bulan Sebelum",
                            df.columns[12]: "Penjualan 1 Bulan Sebelum",
                            df.columns[13]: "Drop6",
                            df.columns[14]: "Awal",
                            df.columns[15]: "Terima",
                            df.columns[16]: "Jual",
                            df.columns[17]: "Free",
                            df.columns[18]: "Koreksi",
                            df.columns[19]: "Retur",
                            df.columns[20]: "Rolling",
                            df.columns[21]: "Akhir",
                            df.columns[22]: "Stok Gudang",
                            df.columns[23]: "Sisa Quota",
                            df.columns[24]: "Kirim Plan",
                            df.columns[25]: "Retur Plan",
                            df.columns[26]: "Rolling ke Plan",
                            df.columns[27]: "Rolling Dari Plan",
                            df.columns[28]: "Siap Kirim",
                            df.columns[29]: "Kirim Aktual",
                            df.columns[30]: "Retur Aktual",
                            df.columns[31]: "Rolling ke Aktual",
                            df.columns[32]: "Rolling Dari Aktual",
                            df.columns[33]: "Keterangan Distributor",
                            df.columns[34]: "Harga Jual",
                            df.columns[35]: "Jenis",
                            df.columns[36]: "Status",
                            df.columns[37]: "Tipe Produk",
                            df.columns[38]: "TOKO",})

    df = df.drop(columns=['Drop1', 'Drop2', 'Drop3', 'Drop4', 'Drop5', 'Drop6'])
    df = df.dropna(subset=['Artikel ID'])
    df['artikel jenis'] = df['Artikel ID'].str[:2]
    df['ukuran'] = df['Artikel ID'].str[8:]
    df['warna'] = df['Artikel ID'].str[7:8]
    df['ARTIKEL'] = df['Artikel ID'].str[:8]
    df['KATEGORI PRODUK'] = df['artikel jenis'].replace(['12','15','22','35','44','45','46','47','51','52','53','54','55','56','57','58','59'],['T-Shirt S/S','T-Shirt L/S','Wangky','Kemeja L/S','Jacket','Jacket','Jacket','Jacket','Celana','Celana','Celana','Celana','Celana','Celana','Celana','Celana','Celana'])
# finish cleaning data stock


# start penjualan & stock merge
# data penjualan merge stok
    df_penjualan = df.iloc[:,[37,6,7,10]].groupby(['KATEGORI PRODUK']).sum().reset_index()
    df_penjualan = df_penjualan.rename(columns={'Jual':'Penjualan Bulan Berjalan'})
    df_penjualan = df_penjualan.loc[(df_penjualan['KATEGORI PRODUK']=='T-Shirt S/S')|(df_penjualan['KATEGORI PRODUK']=='T-Shirt L/S')|(df_penjualan['KATEGORI PRODUK']=='Wangky')|(df_penjualan['KATEGORI PRODUK']=='Kemeja L/S')|(df_penjualan['KATEGORI PRODUK']=='Jacket')|(df_penjualan['KATEGORI PRODUK']=='Celana')]

#data stock ideal & stock aktual merge
    dfa = pd.DataFrame(df.groupby(['TOKO','KATEGORI PRODUK'])['Akhir'].sum()).reset_index()
    dfa = dfa.loc[(dfa['KATEGORI PRODUK']=='T-Shirt S/S')|(dfa['KATEGORI PRODUK']=='T-Shirt L/S')|(dfa['KATEGORI PRODUK']=='Wangky')|(dfa['KATEGORI PRODUK']=='Kemeja L/S')|(dfa['KATEGORI PRODUK']=='Jacket')|(dfa['KATEGORI PRODUK']=='Celana')]
    dfa = pd.DataFrame(dfa.groupby(['TOKO','KATEGORI PRODUK'])['Akhir'].sum()).reset_index()
    dfa = dfa.rename(columns={'Akhir':'STOCK AKTUAL'})
    df_validasi_stock_ideal = pd.DataFrame(df_validasi_stock_ideal.loc[df_validasi_stock_ideal['TOKO']==df.iloc[1][32]].groupby(['KATEGORI PRODUK'])['STOK IDEAL TOKO'].sum()).reset_index()
    df_merge_stock = pd.merge(df_validasi_stock_ideal,dfa,how='inner',on=['KATEGORI PRODUK'])

    def check_stock(x, var1, var2, var3):
        if (x[var2]) > x[var1]+(x[var1]*0.02) :
            x[var3]= 'over stock'
        elif (x[var2]) < x[var1]-(x[var1]*0.02) :
            x[var3]= 'under stock'
        else:
            x[var3]='ideal stock'
        return x   

    df_merge_stock = df_merge_stock.apply(lambda x: check_stock(x, 'STOK IDEAL TOKO', 'STOCK AKTUAL','status check'), axis=1)

    df_penjualan_stock_merge = pd.merge(df_merge_stock,df_penjualan,how='left',on=['KATEGORI PRODUK'])
    df_penjualan_stock_merge = df_penjualan_stock_merge.append(df_penjualan_stock_merge.sum(numeric_only=True), ignore_index=True)
    df_penjualan_stock_merge = df_penjualan_stock_merge.fillna("TOTAL")
    df_penjualan_stock_merge = df_penjualan_stock_merge[['TOKO','KATEGORI PRODUK','STOK IDEAL TOKO','STOCK AKTUAL','status check','Penjualan 2 Bulan Sebelum','Penjualan 1 Bulan Sebelum','Penjualan Bulan Berjalan']]
    # df_penjualan_stock_merge.rename(columns={ df_penjualan.columns[0]: "TOKO",df_penjualan.columns[1]: "KATEGORI PRODUK",df_penjualan.columns[2]: "STOCK IDEAL TOKO",df_penjualan.columns[3]: "STOCK AKTUAL",df_penjualan.columns[4]: "STATUS CHECK",df_penjualan.columns[5]: "PENJUALAN 2 BULAN SEBELUM",df_penjualan.columns[6]: "PENJUALAN 1 BULAN SEBELUM",df_penjualan.columns[7]: "PENJUALAN BERJALAN"}, inplace=True)

# finish penjualan & stock merge


# start check komposisi ukuran
    df_check_ukuran = df.loc[(df['Akhir']>0)|(df['Terima']>0)]
    df_check_ukuran = pd.DataFrame(df_check_ukuran.groupby(['KATEGORI PRODUK','ARTIKEL'])['ukuran'].sum()).reset_index()

    df_check_ukuran_non = df_check_ukuran.loc[(df_check_ukuran['KATEGORI PRODUK']=='T-Shirt S/S')|(df_check_ukuran['KATEGORI PRODUK']=='T-Shirt L/S')|(df_check_ukuran['KATEGORI PRODUK']=='Kemeja L/S')|(df_check_ukuran['KATEGORI PRODUK']=='Jacket')|(df_check_ukuran['KATEGORI PRODUK']=='Wangky')]
    df_check_ukuran_non['jumlah ukuran'] = df_check_ukuran_non['ukuran'].apply(lambda x : len(x)/2)
    df_check_ukuran_non['Status Barang'] = df_check_ukuran_non['ukuran'].apply(lambda x : 'LLMM' in x)
    df_check_ukuran_non = df_check_ukuran_non.loc[(df_check_ukuran_non['jumlah ukuran']<=2)|(df_check_ukuran_non['Status Barang']==False)]

    df_check_ukuran_celana = df_check_ukuran.loc[(df_check_ukuran['KATEGORI PRODUK']=='Celana')]
    df_check_ukuran_celana['jumlah ukuran'] = df_check_ukuran_celana['ukuran'].apply(lambda x : len(x)/2)
    df_check_ukuran_celana = df_check_ukuran_celana.loc[(df_check_ukuran_celana['jumlah ukuran']<=5)]

    df_check_ukuran_fix = pd.concat([df_check_ukuran_non,df_check_ukuran_celana])
    df_check_ukuran_fix = df_check_ukuran_fix.reset_index(drop=True)
    
    # df_check_ukuran = df_check_ukuran.rename(columns={'KATEGORI PRODUK':'KATEGORI PRODUK CHECK','ARTIKEL':'ARTIKEL CHECK','ukuran':'UKURAN CHECK','jumlah ukuran':'JUMLAH UKURAN CHECK'})
    # df_check_ukuran = df_check_ukuran.reset_index(drop=True)
# finish check komposisi ukuran


# start artikel check
    df_artikel = df.loc[(df['Akhir']>0)|(df['Terima']>0)]
    df_artikel = df_artikel[['KATEGORI PRODUK','ARTIKEL']]
    df_artikel = df_artikel.drop_duplicates(subset=['ARTIKEL'])
    df_artikel = df_artikel.loc[(df_artikel['KATEGORI PRODUK']=='T-Shirt S/S')|(df_artikel['KATEGORI PRODUK']=='T-Shirt L/S')|(df_artikel['KATEGORI PRODUK']=='Wangky')|(df_artikel['KATEGORI PRODUK']=='Kemeja L/S')|(df_artikel['KATEGORI PRODUK']=='Jacket')|(df_artikel['KATEGORI PRODUK']=='Celana')]
    df_artikel = df_artikel.rename(columns={'ARTIKEL':'ARTIKEL AKTUAL'})
    df_validasi_artikel_ideal = pd.DataFrame(df_validasi_artikel_ideal.loc[df_validasi_artikel_ideal['TOKO']==df.iloc[1][32]].groupby(['KATEGORI PRODUK'])['QTY/ART'].sum()).reset_index()
    df_artikel = pd.DataFrame(df_artikel.groupby(['KATEGORI PRODUK'])['ARTIKEL AKTUAL'].count()).reset_index()
    df_merge_artikel = pd.merge(df_validasi_artikel_ideal,df_artikel,how='inner',on=['KATEGORI PRODUK'])
    df_merge_artikel = df_merge_artikel.append(df_merge_artikel.sum(numeric_only=True), ignore_index=True)
    df_merge_artikel = df_merge_artikel.fillna("TOTAL")

    def check_artikel(x, var1, var2, var3):
        if (x[var2]) > x[var1] :
            x[var3]= 'over stock'
        elif (x[var2]) < x[var1] :
            x[var3]= 'under stock'
        else:
            x[var3]='ideal stock'
        return x   

    df_merge_artikel_fix = df_merge_artikel.apply(lambda x: check_artikel(x, 'QTY/ART', 'ARTIKEL AKTUAL','status check'), axis=1)
    df_merge_artikel_fix = df_merge_artikel_fix.reset_index(drop=True)
# finish artikel check


# Start Out recommend

    dfda = df.loc[(df['Akhir']>0)|(df['Terima']>0)]
    dfda = dfda.loc[(dfda['KATEGORI PRODUK']=='T-Shirt S/S')|(dfda['KATEGORI PRODUK']=='T-Shirt L/S')|(dfda['KATEGORI PRODUK']=='Wangky')|(dfda['KATEGORI PRODUK']=='Kemeja L/S')|(dfda['KATEGORI PRODUK']=='Jacket')|(dfda['KATEGORI PRODUK']=='Celana')]
    dfda_12 = dfda.groupby(['KATEGORI PRODUK','ARTIKEL'])['Penjualan 3 Bulan Sebelum','Penjualan 2 Bulan Sebelum','Penjualan 1 Bulan Sebelum','Jual','Akhir','Stok Gudang'].sum()

    dfda_12_umur = dfda.groupby(['KATEGORI PRODUK','ARTIKEL'])['Umur'].max()
    dfda_12_umur = pd.DataFrame(dfda_12_umur).reset_index()

    def out_recommend(x, var1, var2, var3):
        if (x[var1]==0)&(x[var2]==0) :
            x[var3] = 'OUT'
        else:
            x[var3] = 'KEEP'
        return x

    df_out = dfda_12.apply(lambda x: out_recommend(x,'Penjualan 2 Bulan Sebelum','Penjualan 1 Bulan Sebelum','Recommend'), axis=1)
    df_out = df_out.sort_values(by=['KATEGORI PRODUK','Recommend','Akhir'],ascending=[False,False,False] )
    df_out = df_out.reset_index()
    df_out = df_out.loc[(df_out['Recommend']=='OUT')]
    df_out = df_out.reset_index(drop=True)

    df_out = pd.merge(df_out,dfda_12_umur,how='left',on=['ARTIKEL'])
    df_out = df_out[['KATEGORI PRODUK_x','ARTIKEL','Penjualan 3 Bulan Sebelum','Penjualan 2 Bulan Sebelum','Penjualan 1 Bulan Sebelum','Jual','Akhir','Stok Gudang','Umur','Recommend']]
    df_out = df_out.rename(columns={'KATEGORI PRODUK_x':'KATEGORI PRODUK'})
    
# Finish out Recommend


# Start check retur
    retur_cek = df.loc[df['ARTIKEL'].isin(df_check_ukuran_fix['ARTIKEL'].to_list())]
    retur_cek = retur_cek[['ARTIKEL','ukuran','Artikel ID','KATEGORI PRODUK','Last Action','Last Action Date','Akhir','Stok Gudang']]
    retur_cek = retur_cek.reset_index(drop=True)

    retur_cek_umur = df.groupby(['KATEGORI PRODUK','Artikel ID'])['Umur'].max()
    retur_cek_umur = pd.DataFrame(retur_cek_umur).reset_index()
    retur_cek_umur = retur_cek_umur[['Artikel ID','Umur']]

    retur_cek_penjualan = df.groupby(['KATEGORI PRODUK','Artikel ID'])['Penjualan 1 Bulan Sebelum','Jual'].sum()
    retur_cek_penjualan['Terjual'] = retur_cek_penjualan['Penjualan 1 Bulan Sebelum']+retur_cek_penjualan['Jual']
    retur_cek_penjualan = retur_cek_penjualan[['Terjual']]
    retur_cek_penjualan = pd.DataFrame(retur_cek_penjualan).reset_index()
    retur_cek_penjualan = retur_cek_penjualan[['Artikel ID','Terjual']]

    retur_cek = pd.merge(retur_cek, retur_cek_umur, how ='left', on=['Artikel ID'])
    retur_cek = pd.merge(retur_cek, retur_cek_penjualan, how ='left', on=['Artikel ID'])

    retur_cek = retur_cek[['ARTIKEL','ukuran','KATEGORI PRODUK','Last Action','Last Action Date','Umur','Terjual','Akhir','Stok Gudang']]
    retur_cek = retur_cek.reset_index(drop=True)
# Finish check retur

    df_penjualan_stock_merge[['---','---']] = np.NaN
    df_merge_artikel_fix[['---','---']] = np.NaN
    df_out[['---','---']] = np.NaN
    df_check_ukuran_fix[['---','---']] = np.NaN

    final_df = pd.concat([df_penjualan_stock_merge,df_merge_artikel_fix],axis=1,ignore_index=False)
    final_final_df = pd.concat([final_df,df_out],axis=1,ignore_index=False)
    final_final_final_df = pd.concat([final_final_df,df_check_ukuran_fix],axis=1,ignore_index=False)
    final_final_final_final_df = pd.concat([final_final_final_df,retur_cek],axis=1,ignore_index=False)
    ffdf = final_final_final_final_df.copy()
    ffdf.rename(columns={ ffdf.columns[0]: "TOKO",
                            ffdf.columns[1]: "KATEGORI PRODUK",
                            ffdf.columns[2]: "STOCK IDEAL TOKO",
                            ffdf.columns[3]: "STOCK AKTUAL",
                            ffdf.columns[4]: "STATUS OVER STOCK",
                            ffdf.columns[5]: "PENJUALAN 2 BULAN SEBELUM",
                            ffdf.columns[6]: "PENJUALAN 1 BULAN SEBELUM",
                            ffdf.columns[7]: "PENJUALAN BULAN BERJALAN",
                            ffdf.columns[8]: "ccccccccccccccc",
                            ffdf.columns[9]: "KATEGORI PRODUK ARTIKEL",
                            ffdf.columns[10]: "ARTIKEL IDEAL",
                            ffdf.columns[11]: "ARTIKEL AKTUAL",
                            ffdf.columns[12]: "STATUS CHECK ARTIKEL",
                            ffdf.columns[13]: "bbbbbbbbb",
                            ffdf.columns[14]: "KATEGORI PRODUK OUT",
                            ffdf.columns[15]: "ARTIKEL OUT",
                            ffdf.columns[16]: "PENJUALAN 3 BULAN OUT",
                            ffdf.columns[17]: "PENJUALAN 2 BULAN OUT",
                            ffdf.columns[18]: "PENJUALAN 1 BULAN OUT",
                            ffdf.columns[19]: "PENJUALAN BERJALAN OUT",
                            ffdf.columns[20]: "AKHIR OUT",
                            ffdf.columns[21]: "STOK GUDANG OUT",
                            ffdf.columns[22]: "UMUR OUT",
                            ffdf.columns[23]: "RECOMMEND",
                            ffdf.columns[24]: "qqqqqqqqqqqq",
                            ffdf.columns[25]: "KATEGORI PRODUK BROKEN",
                            ffdf.columns[26]: "ARTIKEL BROKEN",
                            ffdf.columns[27]: "UKURAN BROKEN",
                            ffdf.columns[28]: "JUMLAH UKURAN BROKEN",
                            ffdf.columns[29]: "STATUS BARANG BROKEN",
                            ffdf.columns[30]: "aaaaaaaaaa",
                            ffdf.columns[31]: "ARTIKEL DETAIL",
                            ffdf.columns[32]: "UKURAN DETAIL",
                            ffdf.columns[33]: "KATEGORI PRODUK DETAIL",
                            ffdf.columns[34]: "LAST ACTION DETAIL",
                            ffdf.columns[35]: "DATE ACTION DETAIL",
                            ffdf.columns[36]: "UMUR DITOKO DETAIL",
                            ffdf.columns[37]: "TERJUAL DARI 2 BULAN AKHIR DETAIL",
                            ffdf.columns[38]: "STOCK AKHIR DETAIL",
                            ffdf.columns[39]: "STOCK GUDANG DETAIL",
                            }
                            , inplace=True)

    return ffdf

data_transform = cheking_2(data1,data2,data2)


st.subheader('data before transform')
st.dataframe(data1)

# st.subheader('data after transform')
# st.dataframe(data_transform)


def to_excel(df):
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, index=False, sheet_name='Sheet1')
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
    format1 = workbook.add_format({'num_format': '0'}) 
    worksheet.set_column('A:A', None, format1)  
    writer.save()
    processed_data = output.getvalue()
    return processed_data

title = st.text_input('')

df_xlsx = to_excel(data_transform)
st.download_button(label='📥 Download Current Result',
                                data=df_xlsx ,
                                file_name= title+'.xlsx')
