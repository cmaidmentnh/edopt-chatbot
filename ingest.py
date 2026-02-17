#!/usr/bin/env python3
"""
EdOpt Chatbot Ingestion Pipeline.
Pulls content from WordPress API + GenCourt DB into local SQLite + generates embeddings.

Usage: python3 ingest.py
"""
import json
import logging
import sys
from datetime import datetime, timezone

from models import (
    init_db, SessionLocal, Provider, ContentPage, StyleTaxonomy,
    RSASection, Legislation, LegislationSponsor, ContentEmbedding,
)
from wp_client import fetch_styles, fetch_all_providers, fetch_all_posts, fetch_all_pages
from gencourt_client import fetch_education_rsas, fetch_current_legislation
from embeddings import generate_batch_embeddings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("ingest")


def ingest_wordpress(db):
    """Pull providers, posts, pages, and styles from EdOpt.org."""
    logger.info("=== Phase 1: WordPress Content ===")

    # Fetch styles taxonomy first
    logger.info("Fetching style taxonomy...")
    styles_dict = fetch_styles()
    for sid, sdata in styles_dict.items():
        existing = db.get(StyleTaxonomy, sid)
        if existing:
            existing.name = sdata["name"]
            existing.slug = sdata["slug"]
            existing.parent_id = sdata["parent_id"]
            existing.description = sdata["description"]
            existing.count = sdata["count"]
        else:
            db.add(StyleTaxonomy(
                id=sid, name=sdata["name"], slug=sdata["slug"],
                parent_id=sdata["parent_id"], description=sdata["description"],
                count=sdata["count"],
            ))
    db.commit()
    logger.info(f"Stored {len(styles_dict)} style taxonomy terms")

    # Fetch providers
    logger.info("Fetching providers...")
    providers = fetch_all_providers(styles_dict)
    for p in providers:
        existing = db.get(Provider, p["id"])
        if existing:
            for key, val in p.items():
                setattr(existing, key, val)
            existing.ingested_at = datetime.now(timezone.utc)
        else:
            db.add(Provider(**p, ingested_at=datetime.now(timezone.utc)))
    db.commit()
    logger.info(f"Stored {len(providers)} providers")

    # Fetch posts
    logger.info("Fetching posts...")
    posts = fetch_all_posts()
    for p in posts:
        existing = db.get(ContentPage, p["id"])
        if existing:
            for key, val in p.items():
                setattr(existing, key, val)
            existing.ingested_at = datetime.now(timezone.utc)
        else:
            db.add(ContentPage(**p, ingested_at=datetime.now(timezone.utc)))
    db.commit()
    logger.info(f"Stored {len(posts)} posts")

    # Fetch pages
    logger.info("Fetching pages...")
    pages = fetch_all_pages()
    for p in pages:
        existing = db.get(ContentPage, p["id"])
        if existing:
            for key, val in p.items():
                setattr(existing, key, val)
            existing.ingested_at = datetime.now(timezone.utc)
        else:
            db.add(ContentPage(**p, ingested_at=datetime.now(timezone.utc)))
    db.commit()
    logger.info(f"Stored {len(pages)} pages")

    return len(providers), len(posts), len(pages)


def ingest_gencourt_rsas(db):
    """Pull education-related RSA sections from GenCourt."""
    logger.info("=== Phase 2: GenCourt RSA Sections ===")

    rsas = fetch_education_rsas()
    for r in rsas:
        existing = db.query(RSASection).filter_by(
            chapter_no=r["chapter_no"], section_no=r["section_no"]
        ).first()
        if existing:
            for key, val in r.items():
                if key != "id":
                    setattr(existing, key, val)
            existing.ingested_at = datetime.now(timezone.utc)
        else:
            db.add(RSASection(
                title_no=r["title_no"], chapter_no=r["chapter_no"],
                section_no=r["section_no"], title_name=r["title_name"],
                chapter_name=r["chapter_name"], section_name=r["section_name"],
                rsa_text=r["rsa_text"], entire_rsa=r["entire_rsa"],
            ))
    db.commit()
    logger.info(f"Stored {len(rsas)} RSA sections")
    return len(rsas)


def ingest_gencourt_legislation(db):
    """Pull current session education bills from GenCourt."""
    logger.info("=== Phase 3: GenCourt Legislation ===")

    bills = fetch_current_legislation()

    for b in bills:
        # Upsert bill
        existing = db.get(Legislation, b["id"])
        if existing:
            existing.bill_number = b["bill_number"]
            existing.title = b["title"]
            existing.session_year = b["session_year"]
            existing.general_status = b["general_status"]
            existing.house_status = b["house_status"]
            existing.senate_status = b["senate_status"]
            existing.subject_code = b["subject_code"]
            existing.bill_text_summary = b["bill_text_summary"]
            existing.committee_name = b["committee_name"]
            existing.next_hearing_date = b["next_hearing_date"]
            existing.next_hearing_room = b["next_hearing_room"]
            existing.docket_summary = b["docket_summary"]
            existing.ingested_at = datetime.now(timezone.utc)
        else:
            db.add(Legislation(
                id=b["id"], bill_number=b["bill_number"], title=b["title"],
                session_year=b["session_year"], general_status=b["general_status"],
                house_status=b["house_status"], senate_status=b["senate_status"],
                subject_code=b["subject_code"], bill_text_summary=b["bill_text_summary"],
                committee_name=b["committee_name"], next_hearing_date=b["next_hearing_date"],
                next_hearing_room=b["next_hearing_room"], docket_summary=b["docket_summary"],
            ))

        # Upsert sponsors (delete old, insert new)
        db.query(LegislationSponsor).filter_by(legislation_id=b["id"]).delete()
        for s in b.get("sponsors", []):
            db.add(LegislationSponsor(
                legislation_id=b["id"],
                person_id=s["person_id"],
                first_name=s["first_name"],
                last_name=s["last_name"],
                party=s["party"],
                district=s["district"],
                legislative_body=s["legislative_body"],
                is_prime_sponsor=s["is_prime_sponsor"],
            ))

    db.commit()
    logger.info(f"Stored {len(bills)} education bills")
    return len(bills)


EFA_HANDBOOK_SECTIONS = [
    {
        "id": 900001,
        "title": "EFA Parent Handbook - Introduction and Overview",
        "content_text": (
            "Education Freedom Accounts (EFA) are grants from the State of New Hampshire made available to families "
            "for their children's education. Qualified educational expenses include tuition at the school of their choice, "
            "tutoring, online learning programs, educational supplies, curriculum, technology, and other educational expenses. "
            "The Children's Scholarship Fund (CSF), a nonprofit charity organization, has been authorized by the State of "
            "New Hampshire to administer these accounts. EFAs are empowering families to personalize their children's "
            "education, allowing NH students to find the learning environment that best fits their needs. "
            "The Parent Handbook is reviewed and updated by CSFNH on or before August 31 each year. "
            "Parents and students are encouraged to visit https://nh.scholarshipfund.org to stay informed. "
            "CSF will email from csfnh@scholarshipfund.org when there are important updates. "
            "CSFNH contact: 180 Loudon Road, Concord, NH 03301. Phone: 603-755-6751. Fax: 844-367-0085. "
            "Email: csfnh@scholarshipfund.org. Website: nh.scholarshipfund.org. Updated November 2025."
        ),
    },
    {
        "id": 900002,
        "title": "EFA Eligibility Requirements",
        "content_text": (
            "To qualify for the Education Freedom Account (EFA) program, families must meet the following criteria: "
            "The parent and student must be New Hampshire residents. "
            "The student must be eligible to enroll in the student's local public elementary or secondary school (K-12). "
            "Students must be 5 years old by September 30 of the given school year. "
            "Families must reapply annually and submit the student's Record of Educational Attainment by July 15 each year. "
            "Parents must sign an annual agreement outlining program terms. "
            "There is NO income limit - universal eligibility as of 2024. "
            "Who is NOT eligible: Students cannot participate if they are enrolled full-time (more than 50% of instructional time) "
            "in a local district public school or a chartered public school, including VLACS. "
            "Students in the special school district within the Department of Corrections (RSA 194:60) are not eligible. "
            "If your child becomes ineligible (e.g., by enrolling full-time in a public or charter school), you must notify "
            "CSF immediately and exit your student by completing the EFA student withdrawal form."
        ),
    },
    {
        "id": 900003,
        "title": "EFA Enrollment Cap and Priority Groups",
        "content_text": (
            "The State legislature has set a maximum enrollment cap of 10,000 EFA students for the 2025-26 school year. "
            "The enrollment cap does not apply to students that meet certain priority guideline groups. "
            "If the EFA program reaches 90% of the enrollment cap in any given year, the cap will be raised by 25% "
            "the following school year (2026-27 to 12,500 students). "
            "Priority Groups (not subject to the annual enrollment cap, may apply at any time during the school year): "
            "1. A student currently enrolled in the EFA program (renewal). "
            "2. A sibling of a student currently enrolled in the EFA program. "
            "3. A child with disabilities as defined by RSA 186-C:2. "
            "4. A student whose family income is less than or equal to 350 percent of the federal poverty guidelines. "
            "Priority group total award amount may be prorated depending on enrollment date."
        ),
    },
    {
        "id": 900004,
        "title": "How to Apply for an EFA",
        "content_text": (
            "Apply for the EFA program directly on CSFNH website at: "
            "https://nh.scholarshipfund.org/apply/nh-education-freedom-accounts/ "
            "If your family has never applied to Children's Scholarship Fund before, click the New Families button. "
            "If your family currently uses an EFA or has applied in the past, click the Returning EFA Families button. "
            "EFA Application Deadlines: Applications are rolling. However, to receive 100% of the EFA grant, your "
            "completed application and supporting materials must be complete and verified by July 15 of the given year. "
            "Establishing Proof of New Hampshire Residency: A P.O. Box may not be used to verify residency. "
            "Acceptable documents include: utility bill, driver's license, DMV State ID, tax return, W2, lease/rental agreement, "
            "mortgage documents, property tax bill, auto registration/insurance, Medicaid/SNAP documentation, "
            "paystub or bank statement, or a signed and notarized Affidavit of Shared Residence from the property owner. "
            "Proving Student(s): acceptable documentation includes child's birth certificate, US passport, State ID, "
            "current SNAP letter with student and parent names, divorce decree, or prior year 1040 with student and parent names. "
            "The application is considered complete once the online application is submitted and all supporting documents "
            "have been verified by CSF. Within 30 days of receipt of a completed student application, CSF will confirm completion."
        ),
    },
    {
        "id": 900005,
        "title": "EFA Differentiated Aid - Additional Funding",
        "content_text": (
            "Qualifying for differentiated aid provides eligible students with additional EFA grant funding, "
            "ranging from an estimated additional $700 to $2,000, for each certain individual factor. "
            "The factors that qualify an eligible student for differentiated aid are: "
            "1. Household income at or below 185 percent of the federal poverty line - families must choose to submit "
            "financial information (Federal 1040 tax return, SNAP documentation, or TANF documentation). "
            "2. English Language Learner - students with a predominant language other than English who are educationally "
            "disadvantaged by limited English proficiency and scored below 4.5 on English language proficiency assessment. "
            "3. Student with a qualifying disability meeting requirements of 34 CFR 300.8. "
            "Note: Qualifying for differentiated aid IS DIFFERENT than qualifying for the EFA priority group "
            "'A child with disabilities as defined by RSA 186-C:2'. "
            "Evidence of eligibility for differentiated aid may be documented by: providing a copy of the student's IEP/ISP "
            "developed by a public school district, providing a signed Medical Certification Form, or providing documentation "
            "from a licensed medical professional certifying the student's diagnosis."
        ),
    },
    {
        "id": 900006,
        "title": "EFA Requirements and Annual Agreement",
        "content_text": (
            "The NH EFA law requires parents/guardians sign an annual agreement attesting and certifying to certain conditions. "
            "The annual agreement includes: EFA funds may only be used for qualifying expenses for the individual student named "
            "on the account. Funds may not be shared between students. Parents are liable for and must return erroneously spent funds. "
            "Parents will provide education in core knowledge domains: science, mathematics, language, government, history, health, "
            "reading, writing, spelling, the history of the constitutions of New Hampshire and the United States, and exposure to "
            "art and music. Parents will provide an annual record of educational attainment by July 15 each year. "
            "Parents agree not to enroll the EFA student full-time in their resident district public school or chartered public school "
            "while participating. If a student transfers to public school full-time, parent must immediately complete "
            "the EFA student withdrawal form at https://csfnh.neonccm.com/familyLogin/index.php?action=efa_exit "
            "Parents agree to notify CSF immediately of any change in residency. "
            "Failure to comply with the annual agreement may result in loss of the EFA."
        ),
    },
    {
        "id": 900007,
        "title": "EFA Homeschoolers Guide",
        "content_text": (
            "If your child is currently registered as a home education student and qualifies for an EFA, your daily routine "
            "may look much the same — but legally, your child will now be considered an EFA student, not a home education student, "
            "under NH law. What changes legally: The EFA program meets NH's compulsory education law requirements, so you no "
            "longer report to your previous participating agency (such as your local school district) for home education compliance. "
            "Instead, CSF will officially report your child as an EFA participant to the NH Department of Education. "
            "You must notify your child's prior participating agency when your student begins participating in EFA. "
            "What stays the same in practice: Parents may still educate their children at home. Families retain control over "
            "curriculum, scheduling, and instruction, provided the education meets the core knowledge domains required by law. "
            "Recordkeeping: Under the EFA program, CSF takes on the responsibility of collecting and reporting enrollment and "
            "compliance with the Annual Record of Educational Attainment requirement to the state (without personally identifying information)."
        ),
    },
    {
        "id": 900008,
        "title": "EFA Funding Schedule and Continuation",
        "content_text": (
            "The State of NH will disburse funds to CSF for the eligible student's EFA account 4 times during the state fiscal year. "
            "Funds become available approximately two weeks after the State releases funds. "
            "EFA disbursement schedule: September - 20%, November - 20%, January - 30%, April - 30%. "
            "Once an EFA is established, the account shall remain open and any unused funds shall roll over from "
            "quarter-to-quarter and from year-to-year until the parent or guardian withdraws the student from the EFA program "
            "or until the EFA student graduates from high school, unless the EFA is closed because of suspected intentional misuse. "
            "Funding Continuation: Parents must reapply and sign the EFA Parent Agreement annually. The EFA law requires that "
            "all students renewing the EFA grant must submit the eligible student's annual record of educational attainment "
            "documents by July 15 in order to maintain the EFA and for CSF to continue requesting the grant from the "
            "NH Department of Education. If a parent fails to provide the annual record by July 15, CSF shall not make any "
            "additional EFA funds available to the EFA student until the record is provided."
        ),
    },
    {
        "id": 900009,
        "title": "EFA Record of Educational Attainment Options",
        "content_text": (
            "What qualifies as a Record of Educational Attainment (due by July 15 annually): "
            "Option 1: A Standardized Test. Acceptable tests include but are not limited to: California Achievement Test, "
            "NWEA/MAP, CLT (Classical Learning Test), ERB-Milestone, ERB-CTP, Iowa Test of Basic Skills, PSAT, SAT, "
            "Stanford Achievement Test, Terra Nova. You will enter the Total Score, Math score, and ELA/Reading score "
            "and upload the report to the student's application by July 15. Many tests can be taken online for a small fee. "
            "Option 2: The NH state-wide assessment. Enter Total Score, Math score, and ELA score and upload by July 15. "
            "Option 3: A signed evaluation letter from a teacher of the student's portfolio of work from the current school year. "
            "A parent/guardian may NOT sign a portfolio evaluation letter for their own child. The letter must include: "
            "teacher name and address with state certification or nonpublic school info, date(s) of evaluation, "
            "description of work reviewed, summary of the child's educational process with a statement about educational progress, "
            "and the teacher's signature. "
            "Option 4: A copy of the student's report card from a public school outside your resident district or nonpublic school "
            "where the student is attending full-time. This option is NOT available to part-time students."
        ),
    },
    {
        "id": 900010,
        "title": "EFA Allowable Uses - Tuition, Online Learning, and Tutoring",
        "content_text": (
            "Allowable Use Categories of the EFA Grant under RSA 194-F:2 II: "
            "Private school tuition and fees (RSA 194-F:2 II.(a)): A private school is a nonpublic school approved for attendance "
            "by the NH Department of Education or the respective state's Department of Education. "
            "List of approved NH nonpublic schools: https://www.education.nh.gov/pathways-education/private-schools "
            "Eligible fees include: academic services fee, activity fee (music, band, etc.), application/enrollment/registration fee, "
            "athletics fee, book fee, technology fee, test fee, uniform fee. "
            "Online learning programs (non-public/private) tuition and fees (RSA 194-F:2 II.(b)): Online courses in core knowledge "
            "domains or holistic comprehensive online school programs. Examples: Acellus Academy, Outschool, Penn Foster High School, "
            "Power Homeschool, Time4Learning. "
            "Tutoring (RSA 194-F:2 II.(c)): Tutoring services by a certified/accredited individual or tutoring facility in the "
            "core knowledge domains: Government, Health, History, Language, Reading, Writing, Spelling, Mathematics, Science. "
            "Public school classes, curricular and co-curricular programs (RSA 194-F:2 II.(c)): May include enrollment at an "
            "out-of-district public school, part-time (50% or less) classes at resident district, tuition for district sports, music, art. "
            "List of approved NH public schools: https://www.education.nh.gov/pathways-education/local-district-schools "
            "Note: Students may not attend full-time classes at a local district public school and enroll in EFA."
        ),
    },
    {
        "id": 900011,
        "title": "EFA Allowable Uses - Curriculum, Materials, and Technology",
        "content_text": (
            "Textbooks, curriculum, or other materials required by the course, program, or lesson (RSA 194-F:2 II.(e)): "
            "EFA funds may be used for textbooks, curriculum, instructional materials, and supplemental materials required for "
            "an approved course, program, or lesson. Textbooks include books, workbooks, educational study materials, digital "
            "material (e-textbooks, e-workbooks, educational videos). Instructional and supplemental materials include: "
            "musical instruments (when required by a curriculum), school supplies (bookbags, backpacks, calculator, lunch box, "
            "math tools, notebooks, folders, binders, printer ink, tape, paperclips, stapler, writing utensils, pens, crayons, "
            "colored pencils, markers), sports equipment (when required by a program, items over $250 may require proof of enrollment). "
            "Computer Device (RSA 194-F:2 II.(f)): Desktop or laptop (including Apple/Mac) - cap $3,500. One computer device "
            "every 3 years using EFA funds. Students should purchase a warranty. Student Computer Device Build also capped at $3,500. "
            "Tablets - cap $1,500 per item (iPad, Android Tablet, Chromebooks, Digital Art/Drawing Tablet, Amazon Fire Tablet, Kindle). "
            "Digital Electronic Devices - cap $750 per item (headphones, microphones, fitness trackers, digital camera, "
            "keyboard/mouse, monitor, printers/3D printers, scanner, webcams). "
            "Internet and technology must be for the individual EFA student's educational needs. General household internet is NOT "
            "allowable. A hot-spot device for the individual student only is allowable. Cell phones and cell phone plans are NOT allowable. "
            "Educational software and applications (RSA 194-F:2 II.(g)): Student versions of Adobe Products, Audio/Video Editing "
            "Software, Microsoft Office, Word processing. "
            "School uniforms (RSA 194-F:2 II.(h)): Only items specifically required by a school's uniform policy. "
            "General clothing or items to meet a dress code are NOT eligible."
        ),
    },
    {
        "id": 900012,
        "title": "EFA Allowable Uses - Assessments, Programs, Therapies, Transportation",
        "content_text": (
            "Fees for assessments and examinations (RSA 194-F:2 II.(i)): Includes nationally standardized assessments, "
            "advanced placement examinations, portfolio evaluations, college/university admission exams, and tuition or fees "
            "for preparatory courses. Acceptable standardized tests: California Achievement Test, CLT, ERB-Milestone, ERB-CTP, "
            "Iowa Test of Basic Skills, NWEA/MAP, PSAT, SAT, Stanford Achievement Test, Terra Nova. "
            "Education programs (RSA 194-F:2 II.(j)): Summer programs, specialized education, health and physical education. "
            "Includes activities where a student participates on a team or enrolls in a class designated as health, physical education, "
            "music, dance, etc. Examples: baseball/softball/T-ball, basketball, cheerleading/gymnastics, dance, football, hockey, "
            "lacrosse/field hockey, martial arts, music lessons/instruction, skiing, soccer/tennis/track, swimming and diving, wrestling. "
            "Career or technical school tuition (RSA 194-F:2 II.(k)): For students attending career/technical or trade school. "
            "Examples: electrical, HVAC, nursing, plumbing, welding. "
            "Special education services and therapies (RSA 194-F:2 II.(l)): Occupational, behavioral, physical, speech-language, "
            "and audiology therapies provided by certified/accredited therapists. "
            "College/university tuition (RSA 194-F:2 II.(m)): For dual-enrolled students receiving both high school and college "
            "credit while in high school. EFA funds may NOT be used for college courses after the child graduates. "
            "Fee-for-service transportation (RSA 194-F:2 II.(n)): Bus companies that transport students to education providers. "
            "Ride services such as Uber or Lyft are NOT eligible. Planes, trains, cruises, boats are NOT eligible."
        ),
    },
    {
        "id": 900013,
        "title": "EFA Prohibited Items and Categories",
        "content_text": (
            "EFA funds may NOT be used for the following items or categories. This list is NOT exhaustive. "
            "If unsure if an item is prohibited, use the ClassWallet Marketplace or email nhapprovals@scholarshipfund.org. "
            "Prohibited: Paying the parent, guardian or immediate family member for their time, expenses or instruction of their "
            "own children (including family memberships, gas, vehicle repairs). "
            "Live animals or accessories for animals. "
            "Blades, knives, non-athletic equipment, weapons, ammunition, archery, bows/crossbows, fishing poles, firearms, marksmanship. "
            "Clothing not specified in a school uniform policy (winter coats, snow pants, boots, underclothing). "
            "College and higher education after the student graduates high school. "
            "Cell phones and cell phone plans. "
            "Farm equipment and housing for live animals (tools, car/truck batteries, solar panels, greenhouses, large hydroponic units, "
            "chicken coops, animal bedding — small/single-user science experiment hydroponic units are OK). "
            "Family memberships (must be student-only; no family gym, museum, YMCA memberships). "
            "Food (candy, gum, culinary ingredients, meal kits, school lunches and snacks). "
            "Household items: electronics (routers, modems, radios, drones, VR headsets, video games, televisions, surround sound), "
            "furniture (book cases, chairs, couches, desks, tables), general (batteries, beauty items, cleaning products, "
            "cookware, kitchen appliances, paper towels, candle making, soap making). "
            "Luxury items (high-end, non-essential goods). Manufacturing machines (CNC, Cricut, kilns, welding/torch, laser engravers). "
            "Medical equipment and supplies (first aid kits, bandages). Multi-user items (flat screen TV, basketball hoop, "
            "home gym machines, home improvement). Preschool expenses (EFA is K-12 only). "
            "Purchase or manufacture of items for resale. Recreational items (bicycles, bouncy houses, camping, swimming pools, "
            "trampolines, exercise machines, swim/scuba gear). "
            "School fees not defined as academic (before/after care, donation fee, fundraising, food/lunch, school store items). "
            "Smart home devices (Amazon Echo, Google Home, Alexa). Streaming services (Roku, Chromecast, Apple TV, Netflix, Amazon Prime). "
            "Theme-park admissions or annual passes. Tickets to live entertainment (concerts, bowling, movies, prom, theater, ballet). "
            "Travel and trips (plane, train, Uber, Lyft, bus tickets, trips outside the country). "
            "Toys (Legos, playsets, action figures, models, stuffed animals, remote control toys)."
        ),
    },
    {
        "id": 900014,
        "title": "How to Access and Spend EFA Grant Funds via ClassWallet",
        "content_text": (
            "EFA grant funds may be accessed by families through the ClassWallet digital wallet and payment platform, "
            "with oversight by CSF. This platform eliminates the need for a paper reimbursement process. "
            "Your real-time EFA student balance is readily accessible and viewable at all times in the digital wallet. "
            "How to Get Started: The digital wallet provider will create a secure user account at CSF's request. "
            "Once the EFA account has been created, the parent will receive a Welcome email from ClassWallet. "
            "Three methods of spending EFA Funds: "
            "1. Marketplace Orders - buy directly from dozens of pre-approved retailers (e.g., Amazon) using EFA funds. "
            "This is the fastest and easiest method. All marketplace orders go through an approval queue. "
            "Note: The system does not allow CSF to delete individual items from your cart - if a prohibited item is included, "
            "the entire order will be rejected and the prohibited item identified so you can reorder. "
            "2. Direct Pay Orders - pay an EFA-approved education service provider or retailer directly from the EFA account. "
            "Fast and efficient. Provider invoices must include: school/provider name, student's name, description of item/service, "
            "date of invoice/service, and amount due (per pupil, not per family). "
            "3. Reimbursement - submit receipts for out-of-pocket purchases. Takes longer due to verification. "
            "Receipts must include: school/provider name, student's name, description, date, and amount paid (per pupil). "
            "All retailer receipts must contain: retailer name, date, description, and amount. "
            "For faster processing and to avoid out-of-pocket payments, use Marketplace or Direct Pay whenever possible. "
            "Contact for item allowability questions: nhapprovals@scholarshipfund.org"
        ),
    },
    {
        "id": 900015,
        "title": "EFA Provider Approval and Education Service Providers",
        "content_text": (
            "All education service providers that provide service or instruction to students must be EFA approved and placed "
            "on the Approved Provider List. This requirement cannot be waived. This includes for reimbursement orders. "
            "A provider is not required to sign up with ClassWallet unless the provider would like to accept EFA payments directly. "
            "Retail vendors are not education service providers and do not need EFA approval (e.g., Target). "
            "How to approve your education service provider: "
            "Step 1: Ask your provider to complete the registration form, including uploading their credentials "
            "(applicable licenses, resume, teaching certifications, work history, proof of education degrees or certificates). "
            "Step 2: After CSF approves the application, the provider will receive an email with a link to the ClassWallet website "
            "to set up their account and verify banking information. "
            "Once verified, you can pay the provider directly from the student's EFA, avoiding out-of-pocket payment. "
            "General purchasing provisions: EFAs are student accounts, not family accounts. Funds may not be shared between students. "
            "CSF reserves the right to request additional documentation at any time to verify allowability. "
            "Orders must be shipped to the verified NH address on file (not shipped out of state). "
            "Most ClassWallet vendors do not ship to P.O. Boxes. "
            "Cash payments to private sellers are not eligible for reimbursement. Gift cards, coupons, point programs not accepted. "
            "Hand-written receipts are not accepted for any reason."
        ),
    },
    {
        "id": 900016,
        "title": "EFA Refunds, Returns, Appeals, and Disqualification",
        "content_text": (
            "Refunds and Returns: EFA funds shall not be refunded, rebated, or shared with a parent, guardian, or EFA student. "
            "Any refund or rebate for goods/services purchased with EFA funds shall be credited directly back to CSF and/or "
            "the ClassWallet account within 30 days. All refunds must be processed through ClassWallet. "
            "For marketplace returns, contact ClassWallet directly: (877) 969-5536 or help@classwallet.com. "
            "Grounds for Disqualification: CSF MUST BE NOTIFIED IMMEDIATELY if: "
            "1. Student enrolls full-time at resident district public school. "
            "2. Student enrolls full-time at a public charter school, including VLACS. "
            "3. Student moves out of state. "
            "Upon any of these events, the student is no longer eligible and you must submit an EFA student withdrawal form. "
            "Process for Appeal of Ineligibility or Priority Status: "
            "Step 1 - Internal Appeal: Email csfnh@scholarshipfund.org with subject 'Appeal for Reconsideration' if your income, "
            "employment, or household information has decreased due to a sudden loss in income as the result of an unexpected "
            "job loss or other life altering event such as death. Quitting your job is not a reason for appeal. "
            "Step 2 - Parent and Education Service Provider Advisory Commission: File a written request within 30 calendar days "
            "of receipt of denied Appeal Notice. Mail to: CSF New Hampshire, Director of Compliance, 180 Loudon Road, Concord, NH 03301. "
            "Suspected Intentional Misuse of EFA Funds: CSF employs a layered approach to detection of suspected fraud. "
            "Each transaction is validated through an approval process. Random audits are conducted throughout the year. "
            "Report suspected fraud at: https://nh.scholarshipfund.org/report-suspected-fraud-or-misuse-of-efa-funds/ "
            "Fraud hotline: English 833-759-7300, Spanish 800-216-1288. "
            "If CSF determines suspected intentional and substantial misuse, it must notify the NH Dept of Education, "
            "State Board of Education, and NH Attorney General within 5 days."
        ),
    },
    {
        "id": 900017,
        "title": "EFA Special Education, Disabilities, and FAPE",
        "content_text": (
            "Special Education Eligibility and NH Education Freedom Accounts (EFA): "
            "Parents of students with special needs should review the document 'Special Education Eligibility and NH EFA' "
            "at https://www.education.nh.gov/sites/g/files/ehbemt326/files/inline-documents/sonh/efa-for-students-with-disabilities_0.pdf "
            "By law (NH Admin Rules Ed 805.01), CSF must notify parents that participation in the EFA program is a parental placement "
            "under 20 USC section 1412, Individuals with Disabilities Education Act (IDEA) if a child with a disability is enrolled "
            "in a non-public school. A child with a disability in an EFA program and enrolled in a public school under "
            "RSA 194-F:2, II(d) is NOT a parental placement under IDEA and shall be entitled to FAPE. "
            "Parentally-placed private school children with disabilities shall not be entitled to a FAPE in connection with their "
            "enrollment by their parents in a private school, in accordance with 34 C.F.R. 300.148(a) and pursuant to 34 C.F.R. 300.137(a), "
            "while participating in the state-funded EFA program. "
            "The school district in which the child with a disability participating in the EFA program enrolled in a public school "
            "under RSA 194-F:2,II(d) resides is responsible for the provision of FAPE. "
            "Medical Certification of Disability: For qualifying for differentiated aid for disability, families may submit "
            "a Medical Certification Form signed by a licensed medical professional. The form requires: Student/Child Name, "
            "Child Date of Birth, Medical Professional information (name, clinic, address, license number, phone), "
            "examination date and location, primary disability category (Autism, Deaf-blindness, Deafness, Developmental Delay, "
            "Emotional Disturbance, Hearing Impairments, Intellectual Disability, Multiple Disabilities, Orthopedic Impairment, "
            "Other Health Impairments, Specific Learning Disability, Speech-Language Impairments, Traumatic Brain Injury, "
            "Visual Impairments), and medical professional's signature under penalty of perjury."
        ),
    },
    {
        "id": 900018,
        "title": "EFA Withdrawal Process and Advisory Commission",
        "content_text": (
            "How to Withdraw a Student from the EFA Program: "
            "The withdrawal form is available at: https://nh.scholarshipfund.org/wp-content/uploads/2025/09/STU-21-Student-Withdrawal-Form.pdf "
            "Reasons for withdrawal include: student graduated high school, student to remain in resident district or charter school "
            "full time, student transferred to NH public school, student transferred to a NH state operated public institution, "
            "student moved out of state, student transferred to a home education program not using EFA funds, "
            "student transferred to a non-public school not using EFA funds. "
            "Direction for Use of Roll Over Funds upon withdrawal: Option 1 - Close the account and forfeit roll over funds. "
            "Option 2 - If the child has been in the EFA program for at least one year, roll over funds continue to be available "
            "until expended (rolled-over funds can continue to be utilized only after the student has been enrolled for one full "
            "school year, and until the former EFA student graduates high school). "
            "Parent and Education Service Provider Advisory Commission: An established parent and education service provider "
            "advisory commission assists CSF by providing recommendations about implementing, administering, and improving the "
            "EFA program. The commission consists of parents of EFA students or education service providers representing no fewer "
            "than 4 counties in the state. Members are appointed by the director of CSF for one calendar year. "
            "The commissioner of the department of education or designee serves as a non-voting member."
        ),
    },
]


def chunk_text(text: str, max_tokens: int = 512) -> list:
    """Split text into chunks of approximately max_tokens words."""
    if not text:
        return []
    words = text.split()
    if len(words) <= max_tokens:
        return [text]
    chunks = []
    for i in range(0, len(words), max_tokens - 50):  # 50-word overlap
        chunk = " ".join(words[i:i + max_tokens])
        if chunk.strip():
            chunks.append(chunk)
    return chunks


def ingest_handbook(db):
    """Ingest EFA Parent Handbook sections as ContentPage entries."""
    logger.info("=== Phase 4: EFA Parent Handbook ===")

    for section in EFA_HANDBOOK_SECTIONS:
        existing = db.get(ContentPage, section["id"])
        if existing:
            existing.title = section["title"]
            existing.content_text = section["content_text"]
            existing.ingested_at = datetime.now(timezone.utc)
        else:
            db.add(ContentPage(
                id=section["id"],
                content_type="handbook",
                slug=f"efa-handbook-{section['id'] - 900000}",
                title=section["title"],
                content_text=section["content_text"],
                url="https://nh.scholarshipfund.org",
                ingested_at=datetime.now(timezone.utc),
            ))
    db.commit()
    logger.info(f"Stored {len(EFA_HANDBOOK_SECTIONS)} EFA handbook sections")
    return len(EFA_HANDBOOK_SECTIONS)


def generate_all_embeddings(db):
    """Generate embeddings for all content in the database."""
    logger.info("=== Phase 4: Generating Embeddings ===")

    # Clear old embeddings
    db.query(ContentEmbedding).delete()
    db.commit()

    all_records = []  # (content_type, content_id, chunk_index, text)

    # Providers
    providers = db.query(Provider).all()
    for p in providers:
        text = f"{p.title}. {p.description or ''}. Styles: {p.styles_raw or ''}. Location: {p.address or ''}"
        all_records.append(("provider", p.id, 0, text.strip()))
    logger.info(f"Prepared {len(providers)} provider embedding texts")

    # Content pages (posts + pages)
    pages = db.query(ContentPage).all()
    for page in pages:
        text = f"{page.title}. {page.content_text or ''}"
        chunks = chunk_text(text)
        for i, chunk in enumerate(chunks):
            all_records.append((page.content_type, page.id, i, chunk))
    logger.info(f"Prepared {sum(1 for r in all_records if r[0] in ('post', 'page'))} content page chunks")

    # RSA sections
    rsas = db.query(RSASection).all()
    for r in rsas:
        text = f"RSA {r.chapter_no}:{r.section_no} - {r.section_name or ''}. {r.chapter_name or ''}. {r.rsa_text or ''}"
        chunks = chunk_text(text)
        for i, chunk in enumerate(chunks):
            all_records.append(("rsa", r.id, i, chunk))
    logger.info(f"Prepared {sum(1 for r in all_records if r[0] == 'rsa')} RSA embedding chunks")

    # Legislation
    bills = db.query(Legislation).all()
    for b in bills:
        sponsors = db.query(LegislationSponsor).filter_by(legislation_id=b.id).all()
        sponsor_names = ", ".join(f"{s.first_name} {s.last_name}" for s in sponsors)
        text = f"{b.bill_number} - {b.title}. Sponsors: {sponsor_names}"
        all_records.append(("legislation", b.id, 0, text.strip()))
    logger.info(f"Prepared {len(bills)} legislation embedding texts")

    # Batch generate embeddings
    logger.info(f"Generating embeddings for {len(all_records)} total chunks...")
    texts = [r[3] for r in all_records]
    embeddings = generate_batch_embeddings(texts)

    # Store in DB
    for (content_type, content_id, chunk_index, text_chunk), emb_bytes in zip(all_records, embeddings):
        db.add(ContentEmbedding(
            content_type=content_type,
            content_id=content_id,
            chunk_index=chunk_index,
            text_chunk=text_chunk,
            embedding=emb_bytes,
        ))

    db.commit()
    logger.info(f"Stored {len(all_records)} embeddings")
    return len(all_records)


def main():
    """Run the full ingestion pipeline."""
    logger.info("Starting EdOpt chatbot ingestion...")
    init_db()

    db = SessionLocal()
    try:
        n_providers, n_posts, n_pages = ingest_wordpress(db)
        n_rsas = ingest_gencourt_rsas(db)
        n_bills = ingest_gencourt_legislation(db)
        n_handbook = ingest_handbook(db)
        n_embeddings = generate_all_embeddings(db)

        summary = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "providers": n_providers,
            "posts": n_posts,
            "pages": n_pages,
            "rsa_sections": n_rsas,
            "legislation": n_bills,
            "handbook_sections": n_handbook,
            "embeddings": n_embeddings,
        }

        with open("last_ingest.json", "w") as f:
            json.dump(summary, f, indent=2)

        logger.info("=== Ingestion Complete ===")
        logger.info(f"Providers: {n_providers}")
        logger.info(f"Posts: {n_posts}")
        logger.info(f"Pages: {n_pages}")
        logger.info(f"RSA Sections: {n_rsas}")
        logger.info(f"Education Bills: {n_bills}")
        logger.info(f"EFA Handbook Sections: {n_handbook}")
        logger.info(f"Total Embeddings: {n_embeddings}")

    except Exception as e:
        logger.error(f"Ingestion failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
