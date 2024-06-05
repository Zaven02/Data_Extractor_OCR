import pandas as pd
import pickle


class DataExtractor:
    def __init__(self, invoice_file, expired_invoices_file):
        self.invoice_file = invoice_file
        self.expired_invoices_file = expired_invoices_file

    def load_data(self):
        print("Loading invoice data from:", self.invoice_file)
        with open(self.invoice_file, 'rb') as file:
            invoices = pickle.load(file)

        print("Loading expired invoice IDs from:", self.expired_invoices_file)
        with open(self.expired_invoices_file, 'r') as file:
            expired_invoices = set(map(int, file.read().strip().split(',')))

        print("Invoice data loaded successfully.")
        return invoices, expired_invoices

    def safe_int(self, value, default=0):
        try:
            if isinstance(value, str):
                value = value.replace('O', '0').replace('o', '0')
            return int(value)
        except (ValueError, TypeError):
            return default

    def safe_datetime(self, value):
        try:
            return pd.to_datetime(value)
        except (ValueError, TypeError):
            return pd.NaT

    def transform_data(self):
        invoices, expired_invoices = self.load_data()
        print("Number of invoices loaded:", len(invoices))
        print("Number of expired invoices loaded:", len(expired_invoices))

        type_mapping = {0: 'Material', 1: 'Equipment', 2: 'Service', 3: 'Other'}

        data = []

        for invoice in invoices:
            invoice_id = self.safe_int(invoice.get('id'), default=None)
            created_on = pd.to_datetime(invoice.get('created_on'), errors='coerce')

            print("Processing invoice ID:", invoice_id)

            if invoice_id is None or pd.isnull(created_on):
                print("Skipping invalid invoice:", invoice)
                continue

            total_price = sum(
                self.safe_int(item.get('unit_price', 0)) * self.safe_int(item.get('quantity', 0)) for item in
                invoice.get('items', []))

            for item in invoice.get('items', []):
                invoiceitem_id = self.safe_int(item.get('id'), default=None)
                if invoiceitem_id is None:
                    continue

                invoiceitem_name = item.get('name', '')
                type_ = type_mapping.get(self.safe_int(item.get('type'), default=3), 'Other')
                unit_price = self.safe_int(item.get('unit_price', 0))
                quantity = self.safe_int(item.get('quantity', 0))
                item_total_price = unit_price * quantity
                percentage_in_invoice = item_total_price / total_price if total_price > 0 else 0
                is_expired = invoice_id in expired_invoices

                data.append({
                    'invoice_id': invoice_id,
                    'created_on': created_on,
                    'invoiceitem_id': invoiceitem_id,
                    'invoiceitem_name': invoiceitem_name,
                    'type': type_,
                    'unit_price': unit_price,
                    'total_price': item_total_price,
                    'percentage_in_invoice': percentage_in_invoice,
                    'is_expired': is_expired
                })

        df = pd.DataFrame(data, columns=[
            'invoice_id', 'created_on', 'invoiceitem_id', 'invoiceitem_name',
            'type', 'unit_price', 'total_price', 'percentage_in_invoice', 'is_expired'
        ])

        print("DataFrame shape:", df.shape)

        df = df.astype({
            'invoice_id': 'int',
            'created_on': 'datetime64[ns]',
            'invoiceitem_id': 'int',
            'invoiceitem_name': 'str',
            'type': 'str',
            'unit_price': 'int',
            'total_price': 'int',
            'percentage_in_invoice': 'float',
            'is_expired': 'bool'
        })

        df = df.sort_values(by=['invoice_id', 'invoiceitem_id'])

        return df

extractor = DataExtractor('invoices_new.pkl', 'expired_invoices.txt')
final_df = extractor.transform_data()
print(final_df.head())
final_df.to_csv('output.csv', index=False)
