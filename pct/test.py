from database import Database
from etl.scraper import Scraper

db = Database(user='rachelwigell', database='pct')
tufts_scraper = Scraper(
    hospital='Tufts',
    page_url='https://www.tuftsmedicalcenter.org/patient-care-services/Request-a-Cost-Estimate',
    link_text='View our chargemaster',
    base_url='https://www.tuftsmedicalcenter.org',
    start_row=2
)
bidmc_scraper = Scraper(
    hospital='BIDMC',
    page_url='https://www.bidmc.org/patient-and-visitor-information/patient-information/your-hospital-bill/price-transparency',
    link_text='View chargemaster',
    base_url='https://www.bidmc.org/'
)
childrens_scraper = Scraper(
    hospital='Childrens',
    page_url='http://www.childrenshospital.org/patient-resources/financial-and-billing-matters/hospital-services-and-charges',
    link_text='Download now',
    base_url='https://www.childrenshospital.org',
    start_row=5
)
dana_farber_scraper = Scraper(
    hospital='Dana_Farber',
    page_url='https://www.dana-farber.org/for-patients-and-families/becoming-a-patient/insurance-and-financial-information/charges-and-your-financial-responsibility/',
    link_text="View Dana-Farber\\'s charges for medical services",
    base_url='https://www.dana-farber.org',
    start_row=2
)
bwh_scraper = Scraper(
    hospital='BWH',
    page_url='https://www.partners.org/for-patients/Patient-Billing-Financial-Assistance/Hospital-Charge-Listing.aspx',
    link_title='BWH hospital charge data zip file',
    sheet_num=0,
    start_row=3
)
mgh_scraper = Scraper(
    hospital='MGH',
    page_url='https://www.partners.org/for-patients/Patient-Billing-Financial-Assistance/Hospital-Charge-Listing.aspx',
    link_title='MGH hospital charge data zip file',
    sheet_num=0,
    start_row=3
)

tufts_scraper.scrape()
bidmc_scraper.scrape()
childrens_scraper.scrape()
dana_farber_scraper.scrape()
bwh_scraper.scrape()
mgh_scraper.scrape()
