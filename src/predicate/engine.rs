use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use common::BitSet;
use serde_json::{json,Value};
use tantivy::{Index,IndexWriter,TantivyDocument,Term,Searcher};
use tantivy::schema::{Schema,Field,IndexRecordOption,STORED,STRING,PLANE,SPHERE,Value as StoredValue};
use tantivy::directory::{Directory,MmapDirectory};
use tantivy::indexer::{merge_indices,NoMergePolicy};
use tantivy::query::{Query,TermQuery,BooleanQuery,Occur,EmptyQuery};
use tantivy::collector::DocSetCollector;
use tantivy::spatial::{cell_index_reader::CellIndexReader,edge_reader::EdgeReader,
    edge_cache::EdgeCache,geometry::Geometry as EngineGeometry,geometry_set::to_geometry_set,
    plane::Plane,sphere::Sphere,surface::Surface,clipper::Clipper,clip_options::ClipOptions,
    region_coverer::CovererOptions,intersects::Intersects,distance::Distance,
    contains::Contains,within::Within,executor::{SpatialExecutor,PlanNode,SpatialRelation}};
use super::case::*;
use super::{Result,write_json};

fn schema(surface:SurfaceKind)->Schema {
    let mut builder=Schema::builder();
    builder.add_text_field("id",STRING|STORED);
    builder.add_text_field("kind",STRING);
    if surface==SurfaceKind::Plane {builder.add_spatial_field("geometry",STORED|PLANE);}
    else {builder.add_spatial_field("geometry",STORED|SPHERE);}
    builder.build()
}

fn writer(index:&Index)->Result<IndexWriter> {
    let writer=index.writer_with_num_threads(1,50_000_000)?;
    writer.set_merge_policy(Box::new(NoMergePolicy));
    Ok(writer)
}

fn fill(index:&Index,case:&Case,partitions:&[Vec<String>],helper:bool,delete:bool)->Result<()> {
    let mut writer=writer(index)?;
    for (n,part) in partitions.iter().enumerate() {
        for id in part {
            let d=case.documents.iter().find(|d|&d.id==id).ok_or("partition ID absent")?;
            let value=json!({"id":d.id,"kind":"candidate","geometry":d.geometry});
            writer.add_document(TantivyDocument::parse_json(&index.schema(),&value.to_string())?)?;
        }
        if helper && n+1==partitions.len() {
            let value=json!({"id":QUERY_ID,"kind":"query","geometry":case.query});
            writer.add_document(TantivyDocument::parse_json(&index.schema(),&value.to_string())?)?;
        }
        writer.commit()?;
    }
    if delete && !case.deleted.is_empty() {
        let id=index.schema().get_field("id")?;
        for d in &case.deleted {writer.delete_term(Term::from_field_text(id,d));}
        writer.commit()?;
    }
    writer.wait_merging_threads()?;
    Ok(())
}

fn build(case:&Case,root:&Path)->Result<Index> {
    let output=root.join("index");std::fs::create_dir(&output)?;
    let schema=schema(case.surface);
    let index=if case.layout==Layout::Segments {
        let index=Index::create_in_dir(&output,schema)?;
        fill(&index,case,&case.partition,case.query_document,true)?;
        index
    } else {
        let mut sources=Vec::new();
        for (i,part) in case.partition.iter().enumerate() {
            let path=root.join(format!("source-{i}"));std::fs::create_dir(&path)?;
            let source=Index::create_in_dir(&path,schema.clone())?;
            fill(&source,case,std::slice::from_ref(part),case.query_document&&i+1==case.partition.len(),case.layout==Layout::Merge)?;
            sources.push((path,source));
        }
        if case.layout==Layout::Merge {
            let inputs:Vec<_>=sources.iter().map(|(_,i)|i.clone()).collect();
            merge_indices(&inputs,MmapDirectory::open(&output)?)?
        } else {
            let index=Index::create_in_dir(&output,schema)?;
            let mut imported=Vec::new();
            for (_,source) in &sources {for segment in source.searchable_segment_metas()? {
                imported.push(index.new_segment_meta(segment.id(),segment.max_doc()));
            }}
            // Writer and destination-owned metadata precede registration, so GC
            // never sees imported component bytes without an owner.
            let mut writer=writer(&index)?;
            for (path,source) in &sources {for segment in source.searchable_segment_metas()? {for name in segment.list_files() {
                if source.directory().exists(&name)? {
                    let bytes=std::fs::read(path.join(&name))?;
                    if bytes.len()>64*1024*1024 {return Err("component exceeds harness copy bound".into());}
                    index.directory().atomic_write(&name,&bytes)?;
                }
            }}}
            for meta in imported {writer.add_segment(meta)?;}
            writer.commit()?;
            if !case.deleted.is_empty() {
                let field=index.schema().get_field("id")?;
                for id in &case.deleted {writer.delete_term(Term::from_field_text(field,id));}
                writer.commit()?;
            }
            writer.wait_merging_threads()?;
            index
        }
    };
    if !index.validate_checksum()?.is_empty(){return Err("index checksum failure".into());}
    // Imported files must survive a fresh writer's normal GC and be managed.
    let gc_writer=writer(&index)?;
    let gc=gc_writer.garbage_collect_files().wait()?;
    if !gc.failed_to_delete_files.is_empty(){return Err("GC failure".into());}
    gc_writer.wait_merging_threads()?;
    let managed=index.directory().list_managed_files();
    for segment in index.searchable_segment_metas()? {for file in segment.list_files() {
        if index.directory().exists(&file)? && !managed.contains(&file){return Err("unmanaged component".into());}
    }}
    drop(index);
    Ok(Index::open_in_dir(output)?)
}

fn raw_geometry(g:&Geometry)->Result<EngineGeometry<Plane>> {
    let value=serde_json::to_value(g)?;
    Ok(EngineGeometry::from_geojson(value.as_object().ok_or("geometry object required")?)?)
}

fn identity(searcher:&Searcher,field:Field,segment:u32,doc:u32)->Result<String> {
    let stored:TantivyDocument=searcher.doc(tantivy::DocAddress::new(segment,doc))?;
    Ok(stored.get_first(field).and_then(|v|v.as_str()).ok_or("stored stable identity absent")?.to_owned())
}

fn stats<S:Surface>(case:&Case,searcher:&Searcher,field:Field)->Result<Value> {
    let geometry=if case.route==Route::Join {
        &case.documents.iter().find(|d|d.id==case.designated_id).ok_or("designated geometry absent")?.geometry
    } else {&case.query};
    let set=to_geometry_set(&raw_geometry(geometry)?.project::<S>(),0);
    let query=Clipper::new(ClipOptions::default()).build(std::slice::from_ref(&set));
    let mut segments=Vec::new();let mut total=BTreeMap::<String,usize>::new();
    for segment in searcher.segment_readers() {
        let spatial=segment.spatial_fields().get_field(field)?.ok_or("spatial data absent")?;
        let cells=CellIndexReader::open(spatial.cells_bytes());
        let mut levels=BTreeMap::<i32,usize>::new();
        for i in 0..cells.cell_count(){*levels.entry(cells.cell_id_at(i).level()).or_default()+=1;}
        let mut seeds=Vec::new();
        for cell in &query.cells {
            let (start,end)=cells.range_for_cell(cell.cell_id,0,cells.cell_count());
            let first=if start<end{Some(cells.cell_id_at(start).level())}else{None};
            for member in &cell.shapes {
                let branch=match first {
                    None=>"skipped",
                    Some(level) if member.edge_indices.is_empty()&&member.contains_center&&level>cell.cell_id.level()=>"bulk",
                    Some(level) if cell.cell_id.level()<level&&!cell.cell_id.is_leaf()=>"split",
                    _=>"candidate",
                };
                *total.entry(branch.into()).or_default()+=1;
                let unsafe_offset=member.geometry_id.1>0 && member.edge_indices.iter().any(|&e|e as usize+1>=set.members[0].vertices.len());
                seeds.push(json!({"cell":cell.cell_id.0.to_string(),"query_level":cell.cell_id.level(),"member":member.geometry_id.1,"index_range":[start,end],"first_index_level":first,"branch":branch,"later_offset_outside_member_zero":unsafe_offset}));
            }
        }
        segments.push(json!({"id":segment.segment_id().uuid_string(),"max_doc":segment.max_doc(),"live_docs":segment.num_docs(),"cells":cells.cell_count(),"levels":levels,"seeds":seeds}));
    }
    Ok(json!({"kind":"recomputed_seed_branches_not_execution_trace","total":total,"segments":segments,
        "sidecar_bulk_eligible":case.route==Route::Direct&&case.sidecar&&matches!(case.filter,Filter::None)&&case.deleted.is_empty()&&!case.empty_inner}))
}

fn direct<S:Surface>(case:&Case,searcher:&Searcher,field:Field,id:Field)->Result<(BTreeSet<String>,bool)> {
    let set=to_geometry_set(&raw_geometry(&case.query)?.project::<S>(),0);
    let mut all=BTreeSet::new();let mut helper=false;
    for (ordinal,segment) in searcher.segment_readers().iter().enumerate() {
        let spatial=segment.spatial_fields().get_field(field)?.ok_or("spatial data absent")?;
        let cells=if case.sidecar{CellIndexReader::open_with_doc_ids(spatial.cells_bytes(),spatial.doc_ids_bytes())}else{CellIndexReader::open(spatial.cells_bytes())};
        let mut cache=EdgeCache::new(vec![EdgeReader::<S>::open(spatial.edges_bytes())],100_000);
        let ids=(0..segment.max_doc()).map(|doc|identity(searcher,id,ordinal as u32,doc)).collect::<Result<Vec<_>>>()?;
        let mut mask=BitSet::with_max_value(segment.max_doc());
        for doc in 0..segment.max_doc(){
            if !segment.is_deleted(doc)&&case.selected(&ids[doc as usize])&&!case.empty_inner{mask.insert(doc);}
        }
        let filter=if matches!(case.filter,Filter::None)&&case.deleted.is_empty()&&!case.empty_inner{None}else{Some(&mask)};
        let options=CovererOptions::default();
        let hits=match case.predicate {
            Predicate::Intersects=>Intersects::new(set.clone(),options).search(&cells,filter,&mut cache,segment.max_doc()),
            Predicate::Distance=>Distance::new(set.clone(),case.radius_radians.unwrap(),options).search(&cells,filter,&mut cache,segment.max_doc()),
            Predicate::Contains=>Contains::new(set.clone(),options).search(&cells,filter,&mut cache,segment.max_doc()),
            Predicate::Within=>Within::new(set.clone(),options).search(&cells,filter,&mut cache,segment.max_doc()),
        };
        for doc in 0..segment.max_doc(){if hits.contains(doc){
            if ids[doc as usize]==QUERY_ID{helper=true;}else{all.insert(ids[doc as usize].clone());}
        }}
    }
    Ok((all,helper))
}

fn term(field:Field,value:&str)->Box<dyn Query>{Box::new(TermQuery::new(Term::from_field_text(field,value),IndexRecordOption::Basic))}
fn join(case:&Case,index:&Index,searcher:&Searcher,field:Field,id:Field)->Result<BTreeSet<String>>{
    let kind=index.schema().get_field("kind")?;
    let mut clauses=vec![(Occur::Must,term(kind,"candidate"))];
    match &case.filter {
        Filter::Include{ids}=>clauses.push((Occur::Must,Box::new(BooleanQuery::union(ids.iter().map(|v|term(id,v)).collect())))),
        Filter::Exclude{ids}=>clauses.push((Occur::MustNot,Box::new(BooleanQuery::union(ids.iter().map(|v|term(id,v)).collect())))),
        _=>{}
    }
    let inner=if case.empty_inner{Box::new(EmptyQuery) as Box<dyn Query>}else{term(kind,"query")};
    let relation=if case.predicate==Predicate::Intersects{SpatialRelation::Intersects}else{SpatialRelation::Near(case.radius_radians.unwrap())};
    let query=SpatialExecutor::new(PlanNode::Join{field,outer:Box::new(PlanNode::Query(Box::new(BooleanQuery::new(clauses)))),inner:Box::new(PlanNode::Query(inner)),relation});
    let addresses=searcher.search(&query,&DocSetCollector)?;
    addresses.into_iter().map(|a|identity(searcher,id,a.segment_ord,a.doc_id)).collect()
}

pub fn run(case:&Case,root:&Path)->Result<Value>{
    if (case.surface==SurfaceKind::Plane&&case.predicate==Predicate::Distance&&case.radius_radians.unwrap()>0.0) ||
       (case.route==Route::Join&&(case.surface!=SurfaceKind::Sphere||!matches!(case.predicate,Predicate::Intersects|Predicate::Distance))) {
        return Ok(json!({"status":"unsupported","reason":"unavailable surface/predicate/route"}));
    }
    write_json(&root.join("stage.json"),&json!({"stage":"index"}))?;
    let index=build(case,root)?;let reader=index.reader()?;let searcher=reader.searcher();
    let field=index.schema().get_field("geometry")?;let id=index.schema().get_field("id")?;
    let statistics=match case.surface{SurfaceKind::Plane=>stats::<Plane>(case,&searcher,field)?,SurfaceKind::Sphere=>stats::<Sphere>(case,&searcher,field)?};
    write_json(&root.join("shape.json"),&statistics)?;
    write_json(&root.join("stage.json"),&json!({"stage":"query"}))?;
    let (hits,helper)=if case.route==Route::Join{(join(case,&index,&searcher,field,id)?,false)}else{
        match case.surface{SurfaceKind::Plane=>direct::<Plane>(case,&searcher,field,id)?,SurfaceKind::Sphere=>direct::<Sphere>(case,&searcher,field,id)?}
    };
    Ok(json!({"status":"completed","actual":hits,"helper_matched":helper,"shape":statistics,
        "documents":searcher.num_docs(),"checksums_valid":true,"managed_gc_passed":true}))
}
