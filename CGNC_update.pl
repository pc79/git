#!/local/bin/perl 


##############################################################################
# Syntax  : perl CGNC_update.pl 
#
# 
###############################################################################

use Time::localtime;
use DBI;

#select last ID (Count is not a good idea, since if some records are deleted you won't get the last ID right)
$dbargs = {AutoCommit => 0, PrintError => 1};
$dbh = DBI->connect("DBI:mysql:database=db_biocurate;host=localhost", "user", "pass", $dbargs);

#$cmd = "select MAX(CAST(Substring(CGNC_ID,2) as Unsigned)) from db_biocurate.tblCGNC where CGNC_ID like 'A%'";
$cmd = "select MAX(CAST(CGNC_ID as Unsigned)) from db_biocurate.tblCGNC";
$sth = $dbh->prepare($cmd);
$sth->execute();
@row = ();
@row = $sth->fetchrow_array;
$count = 0 + $row[0];

#print "A-recrod count: $count\n";


#prepare Entrez GeneIDs with taxon = '9031', this will be used by the GenesNotInCGNC view
#$dbh->do("delete from db_biocurate.tbl_gg_EntrezGeneIDs"); 
#if ($dbh->err()) { die "$DBI::errstr\n"; }
#$dbh->commit();

#$dbh->do("insert into db_biocurate.tbl_gg_EntrezGeneIDs (gene_id) select gene_id from db_biocurate.tbl_gene_info where tax_id = '9031'"); 
#if ($dbh->err()) { die "$DBI::errstr\n"; }
#$dbh->commit();


#select Genes not in CGNC
$dbh->do("delete from db_biocurate.tblTemp_OneColumn"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("insert into db_biocurate.tblTemp_OneColumn (Column1) select Symbol from db_biocurate.GenesNotInCGNC group by Symbol having count(*) > 1"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$cmd = "select * from db_biocurate.GenesNotInCGNC where Symbol not in (select Column1 from db_biocurate.tblTemp_OneColumn)";
$sth2 = $dbh->prepare($cmd);
$sth2->execute();
@row = ();

$tm=localtime;
my ($day,$month,$year)=($tm->mday,$tm->mon+1,$tm->year+1900);
$d = "$year-$month-$day";

while(@row = $sth2->fetchrow_array) 
{
	$count++;
	$id = "$count";
	
	$row[0] =~ s/'/''/g; # Entrez gene ID
	$row[3] =~ s/'/''/g; # name
	$row[7] =~ s/'/''/g; # symbol
	$row[10] =~ s/'/''/g; # synonym
	
	#insert row into tblCGNC
	$dbh->do("INSERT INTO tblCGNC (CGNC_ID, EntrezGene_ID, EntrezGene_version, Ensembl_id, Ensembl_version, Gene_Symbol, Gene_Symbol_from_NomAuth, Gene_Name, Gene_Synonym, Comments, Curation_Status, private_ind, last_edit_date, last_edit_name, biotype_id, Species, ErrorCodes, OrthologyTypeAtApproval) VALUES ('$id', '$row[0]', NULL, NULL, NULL, '$row[7]', '-', '$row[3]', '$row[10]', NULL, '1', '0', '$d', NULL, NULL, 'Gallus gallus', '0', '')"); 
	if ($dbh->err()) { die "$DBI::errstr\n"; }


}
$dbh->commit();



#Update automatic records matching to gene_info records (on EntrezGeneID) without CGNC dbXrefs
$dbh->do("update db_biocurate.tblCGNC c inner join db_biocurate.NonCGNC_Xref_Genes x on x.gene_id = c.EntrezGene_ID set c.Gene_Symbol = x.Symbol, c.Gene_Name = x.Name, c.Gene_Synonym = x.synonyms where c.Curation_Status = '1'"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();




#Remove records with obsolete NCBI gene IDs 
$dbh->do("delete from db_biocurate.tblTemp_OneColumn"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("insert into db_biocurate.tblTemp_OneColumn (Column1) select c.CGNC_ID from db_biocurate.tblEntrezGeneHistory d inner join db_biocurate.tblCGNC c on d.Discontinued_GeneID = c.EntrezGene_ID where d.tax_id = '9031'"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("delete from db_biocurate.tblCGNC where CGNC_ID in (select Column1 from db_biocurate.tblTemp_OneColumn) and Curation_Status = '1'"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

#update comments
#$dbh->do("update db_biocurate.tblCGNC set Comments = concat('Obsolete Entrez Gene ID|', Comments) where CGNC_ID in (select Column1 from db_biocurate.tblTemp_OneColumn)"); 
#if ($dbh->err()) { die "$DBI::errstr\n"; }
#$dbh->commit();

#Update error code 01 - manual record obsolete entrez
$dbh->do("update db_biocurate.tblCGNC set ErrorCodes = concat('01|', ErrorCodes) where CGNC_ID in (select Column1 from db_biocurate.tblTemp_OneColumn) and ErrorCodes <> '0' and ErrorCodes not like '%01%'"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("update db_biocurate.tblCGNC set ErrorCodes = '01' where CGNC_ID in (select Column1 from db_biocurate.tblTemp_OneColumn) and ErrorCodes = '0'"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();


$dbh->do("delete from db_biocurate.tblTemp_OneColumn"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();


#Update Ensembl gene ids based on Ensembl2Gene overwritten by custom mappings
$dbh->do("delete from db_biocurate.tblTemp_Entrez_Ens"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("insert into db_biocurate.tblTemp_Entrez_Ens (Entrez_ID, Ensembl_ID) select distinct geneID, ensembl_gene_id from db_biocurate.tblGene2Ensembl where taxon = '9031'"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("delete from db_biocurate.tblTemp_Entrez_Ens where Entrez_ID in (select Entrez_ID from db_biocurate.tblCGNC_Merged_Entrez_Ens)"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("insert into db_biocurate.tblTemp_Entrez_Ens (Entrez_ID, Ensembl_ID) select distinct Entrez_ID, Ensembl_ID from db_biocurate.tblCGNC_Merged_Entrez_Ens"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("update db_biocurate.tblCGNC c inner join db_biocurate.tblTemp_Entrez_Ens t on t.Entrez_ID = c.EntrezGene_ID set c.Ensembl_id = t.Ensembl_ID"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("delete from db_biocurate.tblTemp_Entrez_Ens"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();




#Remove records with obsolete Ensembl gene IDs 
$dbh->do("delete from db_biocurate.tblCGNC where Ensembl_id not in (select GeneID from db_biocurate.tblEnsemblCurrentGenes) and Curation_Status = '1' and Ensembl_id is not NULL and Ensembl_id <> ''"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

#update comments
#$dbh->do("update db_biocurate.tblCGNC set Comments = concat('Obsolete Ensembl Gene ID|', Comments) where Ensembl_id not in (select GeneID from db_biocurate.tblEnsemblCurrentGenes) and Ensembl_id is not NULL"); 
#if ($dbh->err()) { die "$DBI::errstr\n"; }
#$dbh->commit();

#Update error code 02 - manual record obsolete ensembl
$dbh->do("update db_biocurate.tblCGNC set ErrorCodes = concat('02|', ErrorCodes) where Ensembl_id not in (select GeneID from db_biocurate.tblEnsemblCurrentGenes) and Ensembl_id is not NULL and Ensembl_id <> '' and ErrorCodes <> '0' and ErrorCodes not like '%02%'"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("update db_biocurate.tblCGNC set ErrorCodes = '02' where Ensembl_id not in (select GeneID from db_biocurate.tblEnsemblCurrentGenes) and Ensembl_id is not NULL and Ensembl_id <> '' and ErrorCodes = '0'"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();


#Do HCOP transfers

$dbh->do("update db_biocurate.tblCGNC set Gene_Synonym = '' where Gene_Synonym = '-' or Gene_Synonym is null"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();


#moving LOC* symbol to synonym
$dbh->do("update db_biocurate.tblCGNC c inner join db_biocurate.tblHCOP h on h.chicken_entrez = c.EntrezGene_ID set Gene_Synonym = concat(concat(c.Gene_Synonym, '|'), c.Gene_Symbol) where h.ortholog_type = '1:1' and c.Curation_Status = '1' and (h.human_name <> c.Gene_Name or h.human_symbol <> c.Gene_Symbol) and h.human_symbol <> '-' and h.human_symbol not like 'C%ORF%' and c.Gene_Symbol like 'LOC%' and c.Gene_Synonym <> '' and c.Gene_Synonym not like 'LOC%'"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
#$dbh->commit();

#moving LOC* symbol to empty synonym
$dbh->do("update db_biocurate.tblCGNC c inner join db_biocurate.tblHCOP h on h.chicken_entrez = c.EntrezGene_ID set Gene_Synonym = Gene_Symbol where h.ortholog_type = '1:1' and c.Curation_Status = '1' and (h.human_name <> c.Gene_Name or h.human_symbol <> c.Gene_Symbol) and h.human_symbol <> '-' and h.human_symbol not like 'C%ORF%' and c.Gene_Symbol like 'LOC%' and c.Gene_Synonym = ''"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
#$dbh->commit();

#transfer HCOP data to record with empty Comments
$dbh->do("update db_biocurate.tblHCOP h inner join db_biocurate.tblCGNC c on h.chicken_entrez = c.EntrezGene_ID set c.Gene_Symbol = h.human_symbol, c.Gene_Name = h.human_name, c.Comments = 'Symbol and Name transferred from HCOP' where h.ortholog_type = '1:1' and c.Curation_Status = '1' and (h.human_name <> c.Gene_Name or h.human_symbol <> c.Gene_Symbol) and h.human_symbol <> '-' and h.human_symbol not like 'C%ORF%' and c.Comments is null"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
#$dbh->commit();

#transfer HCOP data to record with non-empty Comments
$dbh->do("update db_biocurate.tblHCOP h inner join db_biocurate.tblCGNC c on h.chicken_entrez = c.EntrezGene_ID set c.Gene_Symbol = h.human_symbol, c.Gene_Name = h.human_name, c.Comments = concat(c.Comments, '|Symbol and Name transferred from HCOP') where h.ortholog_type = '1:1' and c.Curation_Status = '1' and (h.human_name <> c.Gene_Name or h.human_symbol <> c.Gene_Symbol) and h.human_symbol <> '-' and h.human_symbol not like 'C%ORF%' and c.Comments is not null"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();


$dbh->do("update db_biocurate.tblCGNC set Gene_Name = '' where Gene_Name = '-' or Gene_Name is null"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();



#Remove Gene_Name and Gene_Symbol from Gene_Synonym
$dbh->do("update db_biocurate.tblCGNC set Gene_Synonym = replace(Gene_Synonym, concat('|', Gene_Name, '|'), '|') where Gene_Synonym like CONCAT('%|', Gene_Name,'|%')"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("update db_biocurate.tblCGNC set Gene_Synonym = replace(Gene_Synonym, concat('|', Gene_Name), '') where Gene_Synonym like CONCAT('%|', Gene_Name)"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("update db_biocurate.tblCGNC set Gene_Synonym = replace(Gene_Synonym, concat(Gene_Name, '|'), '') where Gene_Synonym like CONCAT(Gene_Name, '|%')"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("update db_biocurate.tblCGNC set Gene_Synonym = replace(Gene_Synonym, concat('|', Gene_Symbol, '|'), '|') where Gene_Synonym like CONCAT('%|', Gene_Symbol,'|%')"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("update db_biocurate.tblCGNC set Gene_Synonym = replace(Gene_Synonym, concat('|', Gene_Symbol), '') where Gene_Synonym like CONCAT('%|', Gene_Symbol)"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("update db_biocurate.tblCGNC set Gene_Synonym = replace(Gene_Synonym, concat(Gene_Symbol, '|'), '') where Gene_Synonym like CONCAT(Gene_Symbol, '|%')"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();


#recalculate error codes

#manual record nolonger 1:1 (code 03)
$dbh->do("update db_biocurate.tblHCOP h inner join db_biocurate.tblCGNC c on h.chicken_entrez = c.EntrezGene_ID set c.ErrorCodes = concat(c.ErrorCodes, '|03') where h.ortholog_type <> '1:1' and c.Curation_Status = '3' and c.OrthologyTypeAtApproval = '1:1' and c.ErrorCodes <> '0' and c.ErrorCodes not like '%03%'"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("update db_biocurate.tblHCOP h inner join db_biocurate.tblCGNC c on h.chicken_entrez = c.EntrezGene_ID set c.ErrorCodes = '03' where h.ortholog_type <> '1:1' and c.Curation_Status = '3' and c.OrthologyTypeAtApproval = '1:1' and c.ErrorCodes = '0'"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();


# For manual records, 1:1 ortholog has different name (code 04)
$dbh->do("update db_biocurate.tblHCOP h inner join db_biocurate.tblCGNC c on h.chicken_entrez = c.EntrezGene_ID set c.ErrorCodes = concat(c.ErrorCodes, '|04') where h.ortholog_type = '1:1' and c.Curation_Status = '3' and (h.human_name <> c.Gene_Name) and h.human_symbol <> '-' and h.human_symbol not like 'C%ORF%' and c.ErrorCodes <> '0' and c.ErrorCodes not like '%04%'"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("update db_biocurate.tblHCOP h inner join db_biocurate.tblCGNC c on h.chicken_entrez = c.EntrezGene_ID set c.ErrorCodes = '04' where h.ortholog_type = '1:1' and c.Curation_Status = '3' and (h.human_name <> c.Gene_Name) and h.human_symbol <> '-' and h.human_symbol not like 'C%ORF%' and c.ErrorCodes = '0'"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();


# For manual records, 1:1 ortholog has different symbol (code 05)
$dbh->do("update db_biocurate.tblHCOP h inner join db_biocurate.tblCGNC c on h.chicken_entrez = c.EntrezGene_ID set c.ErrorCodes = concat(c.ErrorCodes, '|05') where h.ortholog_type = '1:1' and c.Curation_Status = '3' and (h.human_symbol <> c.Gene_Symbol) and h.human_symbol <> '-' and h.human_symbol not like 'C%ORF%' and c.ErrorCodes <> '0' and c.ErrorCodes not like '%05%'"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("update db_biocurate.tblHCOP h inner join db_biocurate.tblCGNC c on h.chicken_entrez = c.EntrezGene_ID set c.ErrorCodes = '05' where h.ortholog_type = '1:1' and c.Curation_Status = '3' and (h.human_symbol <> c.Gene_Symbol) and h.human_symbol <> '-' and h.human_symbol not like 'C%ORF%' and c.ErrorCodes = '0'"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();


# Duplicate symbols (code 06)
$dbh->do("delete from db_biocurate.tblTemp_OneColumn"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("insert into db_biocurate.tblTemp_OneColumn (Column1) select Gene_Symbol from db_biocurate.tblCGNC group by Gene_Symbol having count(*)>1 and Gene_Symbol <> '' and Gene_Symbol is not null"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("update db_biocurate.tblCGNC set ErrorCodes = concat(ErrorCodes, '|06') where Gene_Symbol in (select Column1 from db_biocurate.tblTemp_OneColumn) and ErrorCodes <> '0' and ErrorCodes not like '%06%'"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("update db_biocurate.tblCGNC set ErrorCodes = '06' where Gene_Symbol in (select Column1 from db_biocurate.tblTemp_OneColumn) and ErrorCodes = '0'"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("delete from db_biocurate.tblTemp_OneColumn"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();


# Duplicate entrez id (code 07)
$dbh->do("insert into db_biocurate.tblTemp_OneColumn (Column1) select EntrezGene_ID from db_biocurate.tblCGNC group by EntrezGene_ID having count(*)>1 and EntrezGene_ID <> '' and EntrezGene_ID is not null"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("update db_biocurate.tblCGNC set ErrorCodes = concat(ErrorCodes, '|07') where EntrezGene_ID in (select Column1 from db_biocurate.tblTemp_OneColumn) and ErrorCodes <> '0' and ErrorCodes not like '%07%'"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("update db_biocurate.tblCGNC set ErrorCodes = '07' where EntrezGene_ID in (select Column1 from db_biocurate.tblTemp_OneColumn) and ErrorCodes = '0'"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("delete from db_biocurate.tblTemp_OneColumn"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();


# Duplicate ensembl id (code 08)
$dbh->do("insert into db_biocurate.tblTemp_OneColumn (Column1) select Ensembl_id from db_biocurate.tblCGNC group by Ensembl_id having count(*)>1 and Ensembl_id <> '' and Ensembl_id is not null"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("update db_biocurate.tblCGNC set ErrorCodes = concat(ErrorCodes, '|08') where Ensembl_id in (select Column1 from db_biocurate.tblTemp_OneColumn) and ErrorCodes <> '0' and ErrorCodes not like '%08%'"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("update db_biocurate.tblCGNC set ErrorCodes = '08' where Ensembl_id in (select Column1 from db_biocurate.tblTemp_OneColumn) and ErrorCodes = '0'"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("delete from db_biocurate.tblTemp_OneColumn"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();


# missing symbol (code 09)
$dbh->do("update db_biocurate.tblCGNC set ErrorCodes = concat(ErrorCodes, '|09') where (Gene_Symbol = '' or Gene_Symbol is null) and ErrorCodes <> '0' and ErrorCodes not like '%09%'"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("update db_biocurate.tblCGNC set ErrorCodes = '09' where (Gene_Symbol = '' or Gene_Symbol is null) and ErrorCodes = '0'"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();


# missing name (code 10)
$dbh->do("update db_biocurate.tblCGNC set ErrorCodes = concat(ErrorCodes, '|10') where (Gene_Name = '' or Gene_Name is null) and ErrorCodes <> '0' and ErrorCodes not like '%10%'"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("update db_biocurate.tblCGNC set ErrorCodes = '10' where (Gene_Name = '' or Gene_Name is null) and ErrorCodes = '0'"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();


# Symbol starts with LOC (code 11)
$dbh->do("update db_biocurate.tblCGNC set ErrorCodes = concat(ErrorCodes, '|11') where Gene_Symbol like 'LOC%' and ErrorCodes <> '0' and ErrorCodes not like '%11%'"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("update db_biocurate.tblCGNC set ErrorCodes = '11' where Gene_Symbol like 'LOC%' and ErrorCodes = '0'"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();


# Symbol starts with KIAA (code 12)
$dbh->do("update db_biocurate.tblCGNC set ErrorCodes = concat(ErrorCodes, '|12') where Gene_Symbol like 'KIAA%' and ErrorCodes <> '0' and ErrorCodes not like '%12%'"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();

$dbh->do("update db_biocurate.tblCGNC set ErrorCodes = '12' where Gene_Symbol like 'KIAA%' and ErrorCodes = '0'"); 
if ($dbh->err()) { die "$DBI::errstr\n"; }
$dbh->commit();


$sth->finish();
$sth2->finish();

$dbh->disconnect();
#the end

